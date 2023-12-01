package com.uber.data.kafka.datatransfer.controller.rpc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Command;
import com.uber.data.kafka.datatransfer.CommandType;
import com.uber.data.kafka.datatransfer.HeartbeatRequest;
import com.uber.data.kafka.datatransfer.HeartbeatResponse;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.Participants;
import com.uber.data.kafka.datatransfer.RegisterWorkerRequest;
import com.uber.data.kafka.datatransfer.RegisterWorkerResponse;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.WorkerState;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.common.ZKUtils;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControllerWorkerServiceTest extends FievelTestBase {
  private static Logger logger = LoggerFactory.getLogger(ControllerWorkerServiceTest.class);

  private LeaderSelector leaderSelector;
  private ControllerWorkerService controllerWorkerService;
  private Store<Long, StoredWorker> workerStore;
  private Store<Long, StoredJob> jobStore;
  private Store<Long, StoredJobStatus> jobStatusStore;
  private Node master;
  private JobThroughputSink mockJobThroughputSink;
  private CoreInfra coreInfra;

  @Before
  public void setup() {
    this.coreInfra = CoreInfra.NOOP;
    this.jobStore = Mockito.mock(Store.class);
    this.jobStatusStore = Mockito.mock(Store.class);
    this.workerStore = Mockito.mock(Store.class);
    leaderSelector = Mockito.mock(LeaderSelector.class);
    mockJobThroughputSink = Mockito.mock(JobThroughputSink.class);
    Mockito.when(leaderSelector.isLeader()).thenReturn(true);
    // master does not have ID, do not set it
    this.master = Node.newBuilder().setHost("localhost").setPort(8000).build();
    this.controllerWorkerService =
        new ControllerWorkerService(
            coreInfra,
            master,
            workerStore,
            jobStore,
            jobStatusStore,
            leaderSelector,
            mockJobThroughputSink);
  }

  @Test
  public void registerANewWorkerSuccess() throws Exception {
    StreamObserver<RegisterWorkerResponse> streamObserver = Mockito.mock(StreamObserver.class);
    Node requestedWorkerNode = Node.newBuilder().build();
    Node returnedWorkerNode = Node.newBuilder().setId(1).build();
    Mockito.when(workerStore.create(Mockito.any(), Mockito.any()))
        .thenReturn(
            VersionedProto.from(
                StoredWorker.newBuilder().setNode(returnedWorkerNode).build(),
                ZKUtils.NOOP_VERSION));
    controllerWorkerService.registerWorker(
        RegisterWorkerRequest.newBuilder()
            .setParticipants(Participants.newBuilder().setWorker(requestedWorkerNode).build())
            .build(),
        streamObserver);

    Mockito.verify(workerStore)
        .put(
            Mockito.eq(1L),
            Mockito.eq(
                VersionedProto.from(
                    StoredWorker.newBuilder()
                        .setNode(returnedWorkerNode)
                        .setState(WorkerState.WORKER_STATE_REGISTERING)
                        .build(),
                    ZKUtils.NOOP_VERSION)));
    Mockito.verify(streamObserver)
        .onNext(
            RegisterWorkerResponse.newBuilder()
                .setParticipants(
                    Participants.newBuilder()
                        .setMaster(master)
                        .setWorker(returnedWorkerNode)
                        .build())
                .build());
    Mockito.verify(streamObserver).onCompleted();
  }

  @Test
  public void registerAnOldWorkerSuccess() throws Exception {
    StreamObserver<RegisterWorkerResponse> streamObserver = Mockito.mock(StreamObserver.class);
    Node requestedWorkerNode = Node.newBuilder().setId(1).build();
    Node returnedWorkerNode = Node.newBuilder().setId(2).build();
    Mockito.when(workerStore.create(Mockito.any(), Mockito.any()))
        .thenReturn(
            VersionedProto.from(
                StoredWorker.newBuilder().setNode(returnedWorkerNode).build(),
                ZKUtils.NOOP_VERSION));
    controllerWorkerService.registerWorker(
        RegisterWorkerRequest.newBuilder()
            .setParticipants(Participants.newBuilder().setWorker(requestedWorkerNode).build())
            .build(),
        streamObserver);

    Mockito.verify(workerStore)
        .put(
            Mockito.eq(2L),
            Mockito.eq(
                VersionedProto.from(
                    StoredWorker.newBuilder()
                        .setNode(returnedWorkerNode)
                        .setState(WorkerState.WORKER_STATE_REGISTERING)
                        .build(),
                    ZKUtils.NOOP_VERSION)));
    Mockito.verify(streamObserver)
        .onNext(
            RegisterWorkerResponse.newBuilder()
                .setParticipants(
                    Participants.newBuilder()
                        .setMaster(master)
                        .setWorker(returnedWorkerNode)
                        .build())
                .build());
    Mockito.verify(streamObserver).onCompleted();
  }

  @Test
  public void registerWorkerFailure() throws Exception {
    StreamObserver<RegisterWorkerResponse> streamObserver = Mockito.mock(StreamObserver.class);
    Node workerNode = Node.newBuilder().setId(1).build();
    Mockito.when(workerStore.create(Mockito.any(), Mockito.any()))
        .thenThrow(new TimeoutException("timeout"));
    controllerWorkerService.registerWorker(
        RegisterWorkerRequest.newBuilder()
            .setParticipants(Participants.newBuilder().setWorker(workerNode).build())
            .build(),
        streamObserver);
    Mockito.verify(streamObserver).onError(Mockito.any());
  }

  @Test
  public void registerWorkerNotLeader() throws Exception {
    Mockito.when(leaderSelector.isLeader()).thenReturn(false);
    Mockito.when(leaderSelector.getLeaderId()).thenReturn("localhost:9000");
    StreamObserver<RegisterWorkerResponse> streamObserver = Mockito.mock(StreamObserver.class);
    Node newMaster = Node.newBuilder().setHost("localhost").setPort(9000).build();
    Node workerNode = Node.newBuilder().setId(1).build();
    controllerWorkerService.registerWorker(
        RegisterWorkerRequest.newBuilder()
            .setParticipants(Participants.newBuilder().setWorker(workerNode).build())
            .build(),
        streamObserver);
    Mockito.verify(streamObserver)
        .onNext(
            RegisterWorkerResponse.newBuilder()
                .setParticipants(Participants.newBuilder().setMaster(newMaster).build())
                .build());
    Mockito.verify(streamObserver).onCompleted();
  }

  @Test
  public void registerWorkerNotLeaderButChangeBack() throws Exception {
    Mockito.when(leaderSelector.isLeader()).thenReturn(false);
    Mockito.when(leaderSelector.getLeaderId()).thenReturn("localhost:8000");
    // it works the same
    registerANewWorkerSuccess();
  }

  private static StoredJob buildStoredJob(long jobId, long workerId, JobState expectedState) {
    return buildStoredJob(jobId, workerId, expectedState, 100);
  }

  private static StoredJob buildStoredJob(
      long jobId, long workerId, JobState expectedState, long messageInPerSec) {
    StoredJob.Builder builder = StoredJob.newBuilder();
    builder.getJobBuilder().setJobId(jobId);
    builder.setWorkerId(workerId);
    builder.getJobBuilder().getFlowControlBuilder().setMessagesPerSec(messageInPerSec);

    builder.setState(expectedState);
    return builder.build();
  }

  @Test
  public void heartbeatSuccess() throws Exception {
    long workerId = 2L;
    Node workerNode = Node.newBuilder().setId(workerId).build();
    StreamObserver<HeartbeatResponse> streamObserver = Mockito.mock(StreamObserver.class);
    Job job = Job.newBuilder().setJobId(1L).build();

    // Heartbeat first looks up whether the worker is alive.
    Mockito.when(workerStore.get(Mockito.eq(workerId)))
        .thenReturn(
            VersionedProto.from(
                StoredWorker.newBuilder()
                    .setNode(workerNode)
                    .setState(WorkerState.WORKER_STATE_REGISTERING)
                    .build(),
                ZKUtils.NOOP_VERSION))
        .thenReturn(
            VersionedProto.from(
                StoredWorker.newBuilder()
                    .setNode(workerNode)
                    .setState(WorkerState.WORKER_STATE_WORKING)
                    .build(),
                ZKUtils.NOOP_VERSION));

    Map<Long, Versioned<StoredJob>> expectedJobs =
        ImmutableMap.of(
            1L,
            Versioned.from(
                StoredJob.newBuilder()
                    .setJob(job)
                    .setWorkerId(2L)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build(),
                -1));
    List<JobStatus> actualJobStatuses =
        ImmutableList.of(JobStatus.newBuilder().setJob(job).build());
    Map<Long, Command> commands = new HashMap<>();
    commands.put(
        1L, Command.newBuilder().setJob(job).setType(CommandType.COMMAND_TYPE_RUN_JOB).build());
    Mockito.when(jobStore.getAll(Mockito.any())).thenReturn(expectedJobs);

    controllerWorkerService.heartbeat(
        HeartbeatRequest.newBuilder()
            .setParticipants(Participants.newBuilder().setWorker(workerNode).build())
            .addAllJobStatus(actualJobStatuses)
            .build(),
        streamObserver);
    Mockito.verify(workerStore)
        .putThrough(
            Mockito.eq(workerId),
            Mockito.eq(
                VersionedProto.from(
                    StoredWorker.newBuilder()
                        .setNode(workerNode)
                        .setState(WorkerState.WORKER_STATE_WORKING)
                        .build(),
                    ZKUtils.NOOP_VERSION)));
    Mockito.verify(streamObserver)
        .onNext(
            HeartbeatResponse.newBuilder()
                .setParticipants(
                    Participants.newBuilder().setMaster(master).setWorker(workerNode).build())
                .addAllCommands(commands.values())
                .build());
    Mockito.verify(streamObserver).onCompleted();
    Mockito.verify(mockJobThroughputSink)
        .consume(Mockito.eq(job), Mockito.anyDouble(), Mockito.anyDouble());

    // second heartbeat from working state invokes put instead of putThrough
    controllerWorkerService.heartbeat(
        HeartbeatRequest.newBuilder()
            .setParticipants(Participants.newBuilder().setWorker(workerNode).build())
            .addAllJobStatus(actualJobStatuses)
            .build(),
        streamObserver);
    Mockito.verify(workerStore)
        .put(
            Mockito.eq(workerId),
            Mockito.eq(
                VersionedProto.from(
                    StoredWorker.newBuilder()
                        .setNode(workerNode)
                        .setState(WorkerState.WORKER_STATE_WORKING)
                        .build(),
                    ZKUtils.NOOP_VERSION)));
  }

  @Test
  public void heartbeatFailure() throws Exception {
    StreamObserver<HeartbeatResponse> streamObserver = Mockito.mock(StreamObserver.class);
    Node workerNode = Node.newBuilder().setId(1).build();
    Mockito.when(workerStore.get(ArgumentMatchers.eq(1L)))
        .thenThrow(new TimeoutException("timeout"));
    controllerWorkerService.heartbeat(
        HeartbeatRequest.newBuilder()
            .setParticipants(Participants.newBuilder().setWorker(workerNode).build())
            .build(),
        streamObserver);
    Mockito.verify(streamObserver).onError(Mockito.any());
  }

  @Test
  public void heartbeatNotLeader() throws Exception {
    Mockito.when(leaderSelector.isLeader()).thenReturn(false);
    Mockito.when(leaderSelector.getLeaderId()).thenReturn("localhost:9000");
    StreamObserver<HeartbeatResponse> streamObserver = Mockito.mock(StreamObserver.class);
    Node newMaster = Node.newBuilder().setHost("localhost").setPort(9000).build();
    Node workerNode = Node.newBuilder().setId(1).build();
    controllerWorkerService.heartbeat(
        HeartbeatRequest.newBuilder()
            .setParticipants(Participants.newBuilder().setWorker(workerNode).build())
            .build(),
        streamObserver);
    Mockito.verify(streamObserver)
        .onNext(
            HeartbeatResponse.newBuilder()
                .setParticipants(Participants.newBuilder().setMaster(newMaster).build())
                .build());
    Mockito.verify(streamObserver).onCompleted();
  }

  @Test
  public void heartbeatNotLeaderButChangeBack() throws Exception {
    Mockito.when(leaderSelector.isLeader()).thenReturn(false);
    Mockito.when(leaderSelector.getLeaderId()).thenReturn("localhost:8000");
    // it works the same
    heartbeatSuccess();
  }

  @Test
  public void updateJobStatusNewJobStatus() throws Exception {
    List<JobStatus> jobStatusList = ImmutableList.of(JobStatus.newBuilder().build());
    // get throws exception if not created yet.
    Mockito.doThrow(new RuntimeException()).when(jobStatusStore).get(Mockito.anyLong());
    controllerWorkerService.updateJobStatuses(jobStatusList, 1);
    Mockito.verify(jobStatusStore, Mockito.times(1)).get(Mockito.anyLong());
    Mockito.verify(jobStatusStore, Mockito.times(0)).put(Mockito.anyLong(), Mockito.any());
    Mockito.verify(jobStatusStore, Mockito.times(1)).create(Mockito.any(), Mockito.any());
  }

  @Test
  public void updateJobStatusNewJobStatusThrowsException() throws Exception {
    List<JobStatus> jobStatusList = ImmutableList.of(JobStatus.newBuilder().build());
    // get throws exception if not created yet.
    Mockito.doThrow(new RuntimeException()).when(jobStatusStore).get(Mockito.anyLong());
    Mockito.doThrow(new RuntimeException())
        .when(jobStatusStore)
        .create(Mockito.any(), Mockito.any());
    controllerWorkerService.updateJobStatuses(jobStatusList, 1);
    Mockito.verify(jobStatusStore, Mockito.times(1)).get(Mockito.anyLong());
    Mockito.verify(jobStatusStore, Mockito.times(0)).put(Mockito.anyLong(), Mockito.any());
    Mockito.verify(jobStatusStore, Mockito.times(1)).create(Mockito.any(), Mockito.any());
  }

  @Test
  public void updateJobStatusWithSameJobStatusAndSameWorker() throws Exception {
    int workerId = 1;
    StoredJobStatus storedJobStatus = StoredJobStatus.newBuilder().setWorkerId(workerId).build();
    Mockito.doReturn(Versioned.from(storedJobStatus, 1))
        .when(jobStatusStore)
        .get(Mockito.anyLong());
    controllerWorkerService.updateJobStatuses(
        ImmutableList.of(storedJobStatus.getJobStatus()), workerId);
    Mockito.verify(jobStatusStore, Mockito.times(1)).get(Mockito.anyLong());
    // No put & create invocations, as the store will not be updated with same job status and
    // same worker id
    Mockito.verify(jobStatusStore, Mockito.times(0)).put(Mockito.anyLong(), Mockito.any());
    Mockito.verify(jobStatusStore, Mockito.times(0)).create(Mockito.any(), Mockito.any());
  }

  @Test
  public void updateJobStatusWithSameJobStatusAndDifferentWorker() throws Exception {
    int currentWorkerId = 1;
    int newWorkerId = 2;
    List<JobStatus> jobStatusList = ImmutableList.of(JobStatus.newBuilder().build());
    StoredJobStatus storedJobStatus =
        StoredJobStatus.newBuilder().setWorkerId(currentWorkerId).build();
    Mockito.doReturn(Versioned.from(storedJobStatus, 1))
        .when(jobStatusStore)
        .get(Mockito.anyLong());
    controllerWorkerService.updateJobStatuses(jobStatusList, newWorkerId);
    Mockito.verify(jobStatusStore, Mockito.times(1)).get(Mockito.anyLong());
    Mockito.verify(jobStatusStore, Mockito.times(1)).put(Mockito.anyLong(), Mockito.any());
    Mockito.verify(jobStatusStore, Mockito.times(0)).create(Mockito.any(), Mockito.any());
  }

  @Test
  public void updateJobStatusWithDifferentJobStatusAndSameWorker() throws Exception {
    int workerId = 1;
    JobState currentState = JobState.JOB_STATE_CANCELED;
    JobState newState = JobState.JOB_STATE_RUNNING;
    List<JobStatus> jobStatusList =
        ImmutableList.of(JobStatus.newBuilder().setState(newState).build());
    StoredJobStatus storedJobStatus =
        StoredJobStatus.newBuilder()
            .setJobStatus(JobStatus.newBuilder().setState(currentState).build())
            .setWorkerId(workerId)
            .build();
    Mockito.doReturn(Versioned.from(storedJobStatus, 1))
        .when(jobStatusStore)
        .get(Mockito.anyLong());
    controllerWorkerService.updateJobStatuses(jobStatusList, workerId);
    Mockito.verify(jobStatusStore, Mockito.times(1)).get(Mockito.anyLong());
    Mockito.verify(jobStatusStore, Mockito.times(1)).put(Mockito.anyLong(), Mockito.any());
    Mockito.verify(jobStatusStore, Mockito.times(0)).create(Mockito.any(), Mockito.any());
  }

  @Test
  public void updateJobStatusWithDifferentJobStatusAndDifferentWorker() throws Exception {
    int currentWorkerId = 1;
    int newWorkerId = 2;
    JobState currentState = JobState.JOB_STATE_CANCELED;
    JobState newState = JobState.JOB_STATE_RUNNING;
    List<JobStatus> jobStatusList =
        ImmutableList.of(JobStatus.newBuilder().setState(newState).build());
    StoredJobStatus storedJobStatus =
        StoredJobStatus.newBuilder()
            .setJobStatus(JobStatus.newBuilder().setState(currentState).build())
            .setWorkerId(currentWorkerId)
            .build();
    Mockito.doReturn(Versioned.from(storedJobStatus, 1))
        .when(jobStatusStore)
        .get(Mockito.anyLong());
    controllerWorkerService.updateJobStatuses(jobStatusList, newWorkerId);
    Mockito.verify(jobStatusStore, Mockito.times(1)).get(Mockito.anyLong());
    Mockito.verify(jobStatusStore, Mockito.times(1)).put(Mockito.anyLong(), Mockito.any());
    Mockito.verify(jobStatusStore, Mockito.times(0)).create(Mockito.any(), Mockito.any());
  }

  @Test
  public void updateJobStatusWithExistingJobStatusAndDifferentWorkerThrowsException()
      throws Exception {
    StoredJobStatus storedJobStatus = StoredJobStatus.newBuilder().setWorkerId(1).build();
    List<JobStatus> jobStatusList = ImmutableList.of(JobStatus.newBuilder().build());
    Mockito.doReturn(Versioned.from(storedJobStatus, 1))
        .when(jobStatusStore)
        .get(Mockito.anyLong());
    Mockito.doThrow(new RuntimeException())
        .when(jobStatusStore)
        .put(Mockito.anyLong(), Mockito.any());
    controllerWorkerService.updateJobStatuses(jobStatusList, 2);
    Mockito.verify(jobStatusStore, Mockito.times(1)).get(Mockito.anyLong());
    Mockito.verify(jobStatusStore, Mockito.times(1)).put(Mockito.anyLong(), Mockito.any());
    Mockito.verify(jobStatusStore, Mockito.times(0)).create(Mockito.any(), Mockito.any());
  }
}
