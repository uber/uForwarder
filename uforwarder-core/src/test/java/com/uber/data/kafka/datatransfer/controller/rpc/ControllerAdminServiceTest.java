package com.uber.data.kafka.datatransfer.controller.rpc;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.AddJobGroupRequest;
import com.uber.data.kafka.datatransfer.AddJobGroupResponse;
import com.uber.data.kafka.datatransfer.DeleteJobGroupRequest;
import com.uber.data.kafka.datatransfer.DeleteJobGroupResponse;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.GetAllJobGroupsRequest;
import com.uber.data.kafka.datatransfer.GetAllJobGroupsResponse;
import com.uber.data.kafka.datatransfer.GetJobGroupRequest;
import com.uber.data.kafka.datatransfer.GetJobGroupResponse;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobSnapshot;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.ScaleStatus;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.UpdateJobGroupRequest;
import com.uber.data.kafka.datatransfer.UpdateJobGroupResponse;
import com.uber.data.kafka.datatransfer.UpdateJobGroupStateRequest;
import com.uber.data.kafka.datatransfer.UpdateJobGroupStateResponse;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.controller.autoscalar.AutoScalarConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.stub.StreamObserver;
import java.util.NoSuchElementException;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class ControllerAdminServiceTest extends FievelTestBase {
  private Store<String, StoredJobGroup> jobGroupStore;
  private Store<Long, StoredJobStatus> jobStatusStore;
  private Store<Long, StoredWorker> workerStore;
  private AutoScalarConfiguration autoScalarConfiguration = new AutoScalarConfiguration();
  private ControllerAdminService controllerAdminService;
  private LeaderSelector leaderSelector;

  @Before
  public void setup() {
    jobGroupStore = Mockito.mock(Store.class);
    jobStatusStore = Mockito.mock(Store.class);
    leaderSelector = Mockito.mock(LeaderSelector.class);
    workerStore = Mockito.mock(Store.class);
    Mockito.when(leaderSelector.isLeader()).thenReturn(true);
    Mockito.doReturn("host:1234").when(leaderSelector).getLeaderId();
    controllerAdminService =
        new ControllerAdminService(
            CoreInfra.NOOP,
            jobGroupStore,
            jobStatusStore,
            workerStore,
            autoScalarConfiguration,
            leaderSelector);
  }

  private static JobSnapshot buildJobSnapshot(long jobId, Job job) {
    JobSnapshot.Builder builder = JobSnapshot.newBuilder();
    builder.getExpectedJobBuilder().getJobBuilder().setJobId(jobId);
    builder.getExpectedJobBuilder().setJob(job);
    builder.getActualJobStatusBuilder().getJobStatusBuilder().getJobBuilder().setJobId(jobId);
    return builder.build();
  }

  private static JobSnapshot buildJobSnapshot(long jobId, JobState state) {
    JobSnapshot.Builder builder = JobSnapshot.newBuilder();
    builder.getExpectedJobBuilder().getJobBuilder().setJobId(jobId);
    builder.setActualJobStatus(StoredJobStatus.newBuilder().build());
    builder.getExpectedJobBuilder().setState(state);
    return builder.build();
  }

  @Test
  public void addJobGroupSuccessWhenJobGroupExists() throws Exception {
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder();
    builder.getJobGroupBuilder().setJobGroupId("/dst/src");
    StoredJobGroup createdJobGroup = builder.build();
    JobGroup jobGroupToCreate = createdJobGroup.getJobGroup();
    StoredJobGroup runningJobGroup =
        StoredJobGroup.newBuilder(createdJobGroup).setState(JobState.JOB_STATE_RUNNING).build();
    Mockito.when(jobGroupStore.get(Mockito.any())).thenReturn(Versioned.from(createdJobGroup, 0));
    StreamObserver<AddJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.addJobGroup(
        AddJobGroupRequest.newBuilder()
            .setJobGroup(jobGroupToCreate)
            .setJobGroupState(JobState.JOB_STATE_RUNNING)
            .build(),
        streamObserver);
    Mockito.verify(jobGroupStore, Mockito.times(0)).create(Mockito.any(), Mockito.any());
    Mockito.verify(jobGroupStore, Mockito.times(1)).get(Mockito.any());
    Mockito.verify(jobGroupStore, Mockito.times(0)).put(Mockito.any(), Mockito.any());
    ArgumentCaptor<AddJobGroupResponse> responseCaptor =
        ArgumentCaptor.forClass(AddJobGroupResponse.class);
    Mockito.verify(streamObserver).onNext(responseCaptor.capture());
    Assert.assertEquals(jobGroupToCreate, responseCaptor.getValue().getGroup().getJobGroup());
    Mockito.verify(streamObserver).onCompleted();
  }

  @Test
  public void addJobGroupSuccessWhenJobGroupNotExist() throws Exception {
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder();
    builder.getJobGroupBuilder().setJobGroupId("/dst/src");
    StoredJobGroup createdJobGroup = builder.build();
    JobGroup jobGroupToCreate = createdJobGroup.getJobGroup();
    ScaleStatus scaleStatus =
        ScaleStatus.newBuilder()
            .setScale(1.0)
            .setTotalMessagesPerSec(1000.0)
            .setTotalBytesPerSec(100000.0)
            .build();
    StoredJobGroup runningJobGroup =
        StoredJobGroup.newBuilder(createdJobGroup).setState(JobState.JOB_STATE_RUNNING).build();
    Mockito.when(jobGroupStore.get(Mockito.any())).thenThrow(new NoSuchElementException());
    Mockito.when(jobGroupStore.create(Mockito.any(), Mockito.any()))
        .thenReturn(VersionedProto.from(createdJobGroup, 0));
    StreamObserver<AddJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.addJobGroup(
        AddJobGroupRequest.newBuilder()
            .setJobGroup(jobGroupToCreate)
            .setScaleStatus(scaleStatus)
            .setJobGroupState(JobState.JOB_STATE_RUNNING)
            .build(),
        streamObserver);
    ArgumentCaptor<String> groupIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> jobGroupCaptor =
        ArgumentCaptor.forClass(Versioned.class);
    Mockito.verify(jobGroupStore, Mockito.times(1)).create(Mockito.any(), Mockito.any());
    Mockito.verify(jobGroupStore, Mockito.times(1)).get(Mockito.any());
    Mockito.verify(jobGroupStore).put(groupIdCaptor.capture(), jobGroupCaptor.capture());
    Assert.assertEquals(jobGroupToCreate, jobGroupCaptor.getValue().model().getJobGroup());
    ArgumentCaptor<AddJobGroupResponse> responseCaptor =
        ArgumentCaptor.forClass(AddJobGroupResponse.class);
    Mockito.verify(streamObserver).onNext(responseCaptor.capture());
    Assert.assertEquals(jobGroupToCreate, responseCaptor.getValue().getGroup().getJobGroup());
    Assert.assertEquals(scaleStatus, jobGroupCaptor.getValue().model().getScaleStatus());
    Mockito.verify(streamObserver).onCompleted();
  }

  @Test
  public void addJobGroupNotLeader() {
    Mockito.doReturn(false).when(leaderSelector).isLeader();
    StreamObserver<AddJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.addJobGroup(
        AddJobGroupRequest.newBuilder().setJobGroup(JobGroup.newBuilder().build()).build(),
        streamObserver);
    Mockito.verify(streamObserver).onError(Mockito.any());
  }

  @Test
  public void updateJobGroupConfiguration() throws Exception {
    String jobId = "/dst/src";
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder();
    builder.getJobGroupBuilder().setJobGroupId(jobId);
    builder.setState(JobState.JOB_STATE_RUNNING);
    StoredJobGroup oldJobGroup = builder.build();

    builder
        .getJobGroupBuilder()
        .setFlowControl(FlowControl.newBuilder().setMessagesPerSec(100).build());
    StoredJobGroup newJobGroup = builder.build();
    Mockito.when(jobGroupStore.get(ArgumentMatchers.eq(jobId)))
        .thenReturn(VersionedProto.from(oldJobGroup, 0));
    StreamObserver<UpdateJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.updateJobGroup(
        UpdateJobGroupRequest.newBuilder().setJobGroup(newJobGroup.getJobGroup()).build(),
        streamObserver);
    ArgumentCaptor<String> groupIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> jobGroupCaptor =
        ArgumentCaptor.forClass(Versioned.class);
    Mockito.verify(jobGroupStore).put(groupIdCaptor.capture(), jobGroupCaptor.capture());
    Assert.assertEquals(newJobGroup.getJobGroup(), jobGroupCaptor.getValue().model().getJobGroup());
    ArgumentCaptor<UpdateJobGroupResponse> responseCaptor =
        ArgumentCaptor.forClass(UpdateJobGroupResponse.class);
    Mockito.verify(streamObserver).onNext(responseCaptor.capture());
    Assert.assertEquals(
        newJobGroup.getJobGroup(), responseCaptor.getValue().getGroup().getJobGroup());
    Mockito.verify(streamObserver).onCompleted();
  }

  @Test
  public void updateJobGroupConfigurationNotLeader() {
    Mockito.doReturn(false).when(leaderSelector).isLeader();
    StreamObserver<UpdateJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.updateJobGroup(
        UpdateJobGroupRequest.newBuilder().build(), streamObserver);
    Mockito.verify(streamObserver).onError(Mockito.any());
  }

  @Test
  public void updateJobGroupState() throws Exception {
    String jobId = "/dst/src";
    JobState newJobState = JobState.JOB_STATE_CANCELED;
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder();
    builder.getJobGroupBuilder().setJobGroupId(jobId);
    builder.setState(JobState.JOB_STATE_RUNNING);
    StoredJobGroup oldJobGroup = builder.build();
    StoredJobGroup newJobGroup =
        StoredJobGroup.newBuilder(oldJobGroup).setState(newJobState).build();
    Mockito.when(jobGroupStore.get(ArgumentMatchers.eq(jobId)))
        .thenReturn(VersionedProto.from(oldJobGroup, 0));
    StreamObserver<UpdateJobGroupStateResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.updateJobGroupState(
        UpdateJobGroupStateRequest.newBuilder().setId(jobId).setState(newJobState).build(),
        streamObserver);
    ArgumentCaptor<String> groupIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> jobGroupCaptor =
        ArgumentCaptor.forClass(Versioned.class);
    Mockito.verify(jobGroupStore).put(groupIdCaptor.capture(), jobGroupCaptor.capture());
    Assert.assertEquals(newJobState, jobGroupCaptor.getValue().model().getState());
    ArgumentCaptor<UpdateJobGroupStateResponse> responseCaptor =
        ArgumentCaptor.forClass(UpdateJobGroupStateResponse.class);
    Mockito.verify(streamObserver).onNext(responseCaptor.capture());
    Assert.assertEquals(newJobState, responseCaptor.getValue().getGroup().getState());
    Mockito.verify(streamObserver).onCompleted();
  }

  @Test
  public void updateJobGroupStateNotLeader() {
    Mockito.doReturn(false).when(leaderSelector).isLeader();
    StreamObserver<UpdateJobGroupStateResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.updateJobGroupState(
        UpdateJobGroupStateRequest.newBuilder().build(), streamObserver);
    Mockito.verify(streamObserver).onError(Mockito.any());
  }

  @Test
  public void deleteJobGroup() throws Exception {
    String jobId = "jobGroupOne";
    StreamObserver<DeleteJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.deleteJobGroup(
        DeleteJobGroupRequest.newBuilder().setId(jobId).build(), streamObserver);
    ArgumentCaptor<String> groupIdCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(jobGroupStore).remove(groupIdCaptor.capture());
    Assert.assertEquals(jobId, groupIdCaptor.getValue());
    ArgumentCaptor<DeleteJobGroupResponse> responseCaptor =
        ArgumentCaptor.forClass(DeleteJobGroupResponse.class);
    Mockito.verify(streamObserver).onNext(responseCaptor.capture());
    Assert.assertEquals(DeleteJobGroupResponse.newBuilder().build(), responseCaptor.getValue());
    Mockito.verify(streamObserver).onCompleted();
  }

  @Test
  public void deleteJobGroupNotLeader() {
    Mockito.doReturn(false).when(leaderSelector).isLeader();
    StreamObserver<DeleteJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.deleteJobGroup(
        DeleteJobGroupRequest.newBuilder().build(), streamObserver);
    Mockito.verify(streamObserver).onError(Mockito.any());
  }

  @Test
  public void testGetJobGroupExists() throws Exception {
    String jobId = "jobGroupOne";
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder();
    builder.getJobGroupBuilder().setJobGroupId(jobId);
    builder.setState(JobState.JOB_STATE_RUNNING);
    StoredJobGroup jobGroup = builder.build();
    @SuppressWarnings("unchecked")
    StreamObserver<GetJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    Mockito.doReturn(Versioned.from(jobGroup, -1)).when(jobGroupStore).get(jobId);
    controllerAdminService.getJobGroup(
        GetJobGroupRequest.newBuilder().setId(jobId).build(), streamObserver);
    Mockito.verify(jobGroupStore, Mockito.times(1)).get(jobId);
    Mockito.verify(streamObserver, Mockito.times(1))
        .onNext(GetJobGroupResponse.newBuilder().setGroup(jobGroup).build());
    Mockito.verify(streamObserver, Mockito.times(1)).onCompleted();
    Mockito.verify(streamObserver, Mockito.times(0)).onError(Mockito.any());
  }

  @Test
  public void testGetJobGroupNotExist() throws Exception {
    String jobId = "jobGroupOne";
    Mockito.when(jobGroupStore.get(Mockito.any())).thenThrow(new NoSuchElementException());
    @SuppressWarnings("unchecked")
    StreamObserver<GetJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.getJobGroup(
        GetJobGroupRequest.newBuilder().setId(jobId).build(), streamObserver);
    Mockito.verify(jobGroupStore, Mockito.times(1)).get(jobId);
    Mockito.verify(streamObserver, Mockito.times(0)).onNext(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(0)).onCompleted();
    Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
  }

  @Test
  public void testGetJobGroupNotLeader() throws Exception {
    String jobId = "jobGroupOne";
    Mockito.doReturn(false).when(leaderSelector).isLeader();
    @SuppressWarnings("unchecked")
    StreamObserver<GetJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.getJobGroup(
        GetJobGroupRequest.newBuilder().setId(jobId).build(), streamObserver);
    Mockito.verify(streamObserver).onError(Mockito.any());
  }

  @Test
  public void testGetAllJobGroups() throws Exception {
    String jobId = "jobGroupOne";
    @SuppressWarnings("unchecked")
    StreamObserver<GetAllJobGroupsResponse> streamObserver = Mockito.mock(StreamObserver.class);
    Mockito.doReturn(
            ImmutableMap.of(
                "jobGroupOne", Versioned.from(StoredJobGroup.newBuilder().build(), -1),
                "jobGroupTwo", Versioned.from(StoredJobGroup.newBuilder().build(), -1)))
        .when(jobGroupStore)
        .getAll();
    controllerAdminService.getAllJobGroups(
        GetAllJobGroupsRequest.newBuilder().build(), streamObserver);
    Mockito.verify(jobGroupStore, Mockito.times(1)).getAll();
    Mockito.verify(streamObserver, Mockito.times(2)).onNext(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(1)).onCompleted();
    Mockito.verify(streamObserver, Mockito.times(0)).onError(Mockito.any());
  }

  @Test
  public void testGetAllJobGroupsNotLeader() {
    Mockito.doReturn(false).when(leaderSelector).isLeader();
    StreamObserver<DeleteJobGroupResponse> streamObserver = Mockito.mock(StreamObserver.class);
    controllerAdminService.deleteJobGroup(
        DeleteJobGroupRequest.newBuilder().build(), streamObserver);
    Mockito.verify(streamObserver).onError(Mockito.any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetClusterScaleStatus() throws Exception {
    autoScalarConfiguration.setMessagesPerSecPerWorker(4000L);
    StoredWorker workingWorker =
        StoredWorker.newBuilder()
            .setState(com.uber.data.kafka.datatransfer.WorkerState.WORKER_STATE_WORKING)
            .build();
    Mockito.when(workerStore.getAll(Mockito.any()))
        .thenReturn(ImmutableMap.of(1L, VersionedProto.from(workingWorker, 0)));
    StoredJobGroup runningJobGroup =
        StoredJobGroup.newBuilder()
            .setState(JobState.JOB_STATE_RUNNING)
            .setScaleStatus(
                com.uber.data.kafka.datatransfer.ScaleStatus.newBuilder()
                    .setTotalBytesPerSec(123.0)
                    .setTotalMessagesPerSec(456.0)
                    .build())
            .build();
    Mockito.when(jobGroupStore.getAll(Mockito.any()))
        .thenReturn(ImmutableMap.of("g1", VersionedProto.from(runningJobGroup, 0)));
    StreamObserver<com.uber.data.kafka.datatransfer.GetClusterScaleStatusResponse> streamObserver =
        Mockito.mock(StreamObserver.class);
    controllerAdminService.getClusterScaleStatus(
        com.uber.data.kafka.datatransfer.GetClusterScaleStatusRequest.newBuilder().build(),
        streamObserver);
    ArgumentCaptor<com.uber.data.kafka.datatransfer.GetClusterScaleStatusResponse> responseCaptor =
        ArgumentCaptor.forClass(
            com.uber.data.kafka.datatransfer.GetClusterScaleStatusResponse.class);
    Mockito.verify(streamObserver).onNext(responseCaptor.capture());
    com.uber.data.kafka.datatransfer.GetClusterScaleStatusResponse response =
        responseCaptor.getValue();
    Assert.assertEquals(1, response.getTotalWorkerCount());
    Assert.assertEquals(1, response.getTotalJobgroupCount());
    Assert.assertEquals(4000.0, response.getTotalAllowedMessagesPerSec(), 0.0001);
    Assert.assertEquals(123.0, response.getTotalUsedBytesPerSec(), 0.0001);
    Assert.assertEquals(456.0, response.getTotalUsedMessagesPerSec(), 0.0001);
    Mockito.verify(streamObserver).onCompleted();
    Mockito.verify(streamObserver, Mockito.never()).onError(Mockito.any());

    autoScalarConfiguration.setMessagesPerSecPerWorker(Long.MAX_VALUE);
    Mockito.when(workerStore.getAll(Mockito.any()))
        .thenReturn(
            ImmutableMap.of(
                1L,
                VersionedProto.from(workingWorker, 0),
                2L,
                VersionedProto.from(workingWorker, 0)));
    controllerAdminService.getClusterScaleStatus(
        com.uber.data.kafka.datatransfer.GetClusterScaleStatusRequest.newBuilder().build(),
        streamObserver);
    Mockito.verify(streamObserver, Mockito.times(2)).onNext(responseCaptor.capture());
    response = responseCaptor.getValue();
    Assert.assertEquals(1.8446744073709552E19, response.getTotalAllowedMessagesPerSec(), 0.0001);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetClusterScaleStatusWithException() throws Exception {
    Mockito.when(workerStore.getAll(Mockito.any()))
        .thenThrow(new RuntimeException("Failed to get workers"));
    StreamObserver<com.uber.data.kafka.datatransfer.GetClusterScaleStatusResponse> streamObserver =
        Mockito.mock(StreamObserver.class);
    controllerAdminService.getClusterScaleStatus(
        com.uber.data.kafka.datatransfer.GetClusterScaleStatusRequest.newBuilder().build(),
        streamObserver);
    Mockito.verify(streamObserver, Mockito.never()).onNext(Mockito.any());
    Mockito.verify(streamObserver, Mockito.never()).onCompleted();
    Mockito.verify(streamObserver)
        .onError(
            Mockito.argThat(
                t ->
                    t instanceof io.grpc.StatusRuntimeException
                        && ((io.grpc.StatusRuntimeException) t).getStatus().getCode()
                            == io.grpc.Status.Code.INTERNAL
                        && ((io.grpc.StatusRuntimeException) t)
                            .getStatus()
                            .getDescription()
                            .equals("Failed to get cluster scale status")));
  }
}
