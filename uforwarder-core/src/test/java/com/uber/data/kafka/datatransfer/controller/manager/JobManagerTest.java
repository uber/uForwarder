package com.uber.data.kafka.datatransfer.controller.manager;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.ScaleStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.data.kafka.datatransfer.controller.rebalancer.ShadowRebalancerDelegate;
import com.uber.data.kafka.datatransfer.controller.storage.IdProvider;
import com.uber.data.kafka.datatransfer.controller.storage.LocalSequencer;
import com.uber.data.kafka.datatransfer.controller.storage.LocalStore;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class JobManagerTest extends FievelTestBase {
  private JobManager jobManager;
  private Store<String, StoredJobGroup> jobGroupStore;
  private Store<Long, StoredWorker> workerStore;
  private Rebalancer rebalancer;

  private ShadowRebalancerDelegate shadowRebalancerDelegate;
  private LeaderSelector leaderSelector;
  private String jobGroupKey;
  private StoredJobGroup jobGroup;
  private JobGroup expectedJobGroup;

  private Store<Long, StoredJobStatus> jobStatusStore;

  private static StoredJob buildJob(long jobId, long workerId, JobState state, double scale) {
    StoredJob.Builder builder = StoredJob.newBuilder();
    builder.getJobBuilder().setJobId(jobId);
    builder
        .getJobBuilder()
        .setFlowControl(FlowControl.newBuilder().setMessagesPerSec(1.0d).build());
    builder.getJobBuilder().setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER);
    builder.setWorkerId(workerId);
    builder.setState(state);
    builder.setScale(scale);
    return builder.build();
  }

  private static StoredWorker buildWorker(long workerId) {
    StoredWorker.Builder builder = StoredWorker.newBuilder();
    builder.getNodeBuilder().setId(workerId);
    return builder.build();
  }

  @Before
  public void setup() throws Exception {
    jobGroupKey = "jobGroup";
    expectedJobGroup =
        JobGroup.newBuilder()
            .setJobGroupId(jobGroupKey)
            .setFlowControl(FlowControl.newBuilder().setMessagesPerSec(3.0).build())
            .build();
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.setJobGroup(expectedJobGroup);
    jobGroupBuilder.addJobs(buildJob(1L, 1L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroupBuilder.addJobs(buildJob(2L, 3L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroupBuilder.addJobs(buildJob(3L, 0L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroup = jobGroupBuilder.build();

    leaderSelector = Mockito.mock(LeaderSelector.class);
    Mockito.when(leaderSelector.isLeader()).thenReturn(true);

    rebalancer = Mockito.mock(Rebalancer.class);
    shadowRebalancerDelegate = Mockito.mock(ShadowRebalancerDelegate.class);
    workerStore = Mockito.mock(Store.class);
    Map<Long, StoredWorker> workers = ImmutableMap.of(1L, buildWorker(1), 2L, buildWorker(2));
    Mockito.when(workerStore.getAll())
        .thenReturn(
            workers.entrySet().stream()
                .collect(
                    Collectors.toMap(Map.Entry::getKey, e -> Versioned.from(e.getValue(), 0))));
    jobGroupStore = Mockito.mock(Store.class);
    Mockito.when(jobGroupStore.getAll())
        .thenReturn(ImmutableMap.of(jobGroupKey, VersionedProto.from(jobGroup)));
    jobStatusStore = Mockito.mock(Store.class);
    Mockito.when(jobStatusStore.getAll())
        .thenReturn(ImmutableMap.of(1L, VersionedProto.from(StoredJobStatus.newBuilder().build())));

    jobManager =
        new JobManager(
            new NoopScope(),
            jobGroupStore,
            jobStatusStore,
            workerStore,
            rebalancer,
            shadowRebalancerDelegate,
            leaderSelector);
  }

  @Test
  public void testRebalanceJobGroupsUnsetStaleWorker() throws Exception {
    jobManager.rebalanceJobGroups();

    // Verify that the correct data was written to ZK.
    ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> itemCaptor = ArgumentCaptor.forClass(Versioned.class);
    Mockito.verify(jobGroupStore, Mockito.times(1)).put(idCaptor.capture(), itemCaptor.capture());
    Assert.assertEquals(jobGroupKey, idCaptor.getValue());
    StoredJobGroup.Builder expectedJobGroupBuilder = StoredJobGroup.newBuilder();
    expectedJobGroupBuilder.getJobGroupBuilder().setJobGroupId(jobGroupKey);
    expectedJobGroupBuilder
        .getJobGroupBuilder()
        .setFlowControl(FlowControl.newBuilder().setMessagesPerSec(3.0).build());
    expectedJobGroupBuilder.addJobs(buildJob(1, 1, JobState.JOB_STATE_RUNNING, 0d));
    expectedJobGroupBuilder.addJobs(buildJob(2, 0, JobState.JOB_STATE_RUNNING, 0d));
    expectedJobGroupBuilder.addJobs(buildJob(3, 0, JobState.JOB_STATE_RUNNING, 0d));
    assertEqualsWithoutTimestamp(expectedJobGroupBuilder.build(), itemCaptor.getValue().model());
  }

  @Test
  public void testRebalanceJobGroupsNoOpWithShadowRebalancer() throws Exception {
    // override the job group.
    jobGroupKey = "jobGroup";
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.getJobGroupBuilder().setJobGroupId(jobGroupKey);
    jobGroupBuilder.addJobs(buildJob(1L, 1L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroupBuilder.addJobs(buildJob(2L, 2L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroup = jobGroupBuilder.build();
    Mockito.when(jobGroupStore.getAll())
        .thenReturn(ImmutableMap.of(jobGroupKey, VersionedProto.from(jobGroup)));

    jobManager.rebalanceJobGroupsWithShadowRebalancer();

    // Verify that there is no write to the data store.
    Mockito.verify(jobGroupStore, Mockito.never())
        .put(ArgumentMatchers.anyString(), ArgumentMatchers.any());
  }

  @Test
  public void testRebalanceJobGroupsWithFailureWithShadowRebalancer() throws Exception {
    AtomicInteger computeJobConfigCounter = new AtomicInteger(0);
    ShadowRebalancerDelegate runningDelegate =
        new ShadowRebalancerDelegate() {
          @Override
          public boolean runShadowRebalancer() {
            return true;
          }

          @Override
          public void computeLoad(final Map<String, RebalancingJobGroup> jobGroupMap) {
            throw new RuntimeException();
          }

          @Override
          public void computeJobConfiguration(
              final Map<String, RebalancingJobGroup> jobGroups,
              final Map<Long, StoredWorker> workers) {
            computeJobConfigCounter.incrementAndGet();
          }
        };

    JobManager jobManager =
        new JobManager(
            new NoopScope(),
            jobGroupStore,
            jobStatusStore,
            workerStore,
            rebalancer,
            runningDelegate,
            leaderSelector);
    // override the job group.
    jobGroupKey = "jobGroup";
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.getJobGroupBuilder().setJobGroupId(jobGroupKey);
    jobGroupBuilder.addJobs(buildJob(1L, 1L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroupBuilder.addJobs(buildJob(2L, 2L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroup = jobGroupBuilder.build();
    Mockito.when(jobGroupStore.getAll())
        .thenReturn(ImmutableMap.of(jobGroupKey, VersionedProto.from(jobGroup)));

    jobManager.rebalanceJobGroupsWithShadowRebalancer();

    // Verify that there is no write to the data store.
    Mockito.verify(jobGroupStore, Mockito.never())
        .put(ArgumentMatchers.anyString(), ArgumentMatchers.any());
    Assert.assertEquals(0, computeJobConfigCounter.get());
  }

  @Test
  public void testRebalanceJobGroupsNoChangeWithShadowRebalancer() throws Exception {
    ShadowRebalancerDelegate runningDelegate =
        new ShadowRebalancerDelegate() {
          @Override
          public boolean runShadowRebalancer() {
            return true;
          }
        };

    JobManager jobManager =
        new JobManager(
            new NoopScope(),
            jobGroupStore,
            jobStatusStore,
            workerStore,
            rebalancer,
            runningDelegate,
            leaderSelector);
    // override the job group.
    jobGroupKey = "jobGroup";
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.getJobGroupBuilder().setJobGroupId(jobGroupKey);
    jobGroupBuilder.addJobs(buildJob(1L, 1L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroupBuilder.addJobs(buildJob(2L, 2L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroup = jobGroupBuilder.build();
    Mockito.when(jobGroupStore.getAll())
        .thenReturn(ImmutableMap.of(jobGroupKey, VersionedProto.from(jobGroup)));

    jobManager.rebalanceJobGroupsWithShadowRebalancer();

    // Verify that there is no write to the data store.
    Mockito.verify(jobGroupStore, Mockito.never())
        .put(ArgumentMatchers.anyString(), ArgumentMatchers.any());
  }

  @Test
  public void testRebalanceJobGroupsNoChange() throws Exception {
    // override the job group.
    jobGroupKey = "jobGroup";
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.getJobGroupBuilder().setJobGroupId(jobGroupKey);
    jobGroupBuilder.addJobs(buildJob(1L, 1L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroupBuilder.addJobs(buildJob(2L, 2L, JobState.JOB_STATE_RUNNING, 0d));
    jobGroup = jobGroupBuilder.build();
    Mockito.when(jobGroupStore.getAll())
        .thenReturn(ImmutableMap.of(jobGroupKey, VersionedProto.from(jobGroup)));

    jobManager.rebalanceJobGroups();

    // Verify that there is no write to the data store.
    Mockito.verify(jobGroupStore, Mockito.never())
        .put(ArgumentMatchers.anyString(), ArgumentMatchers.any());
  }

  /**
   * Verify that job group rebalances correctly when rebalancer returns new worker assigment ma0.
   */
  @Test
  public void testRebalanceJobGroupsWithWorkerIdChange() throws Exception {
    Mockito.doAnswer(
            invocation -> {
              Map<String, RebalancingJobGroup> jobGroupMap = invocation.getArgument(0);
              Map<Long, StoredWorker> workerMap = invocation.getArgument(0);
              RebalancingJobGroup rebalancingJobGroup = jobGroupMap.get(jobGroupKey);
              rebalancingJobGroup.updateJob(
                  2L, rebalancingJobGroup.getJobs().get(2L).toBuilder().setWorkerId(2L).build());
              rebalancingJobGroup.updateJob(
                  3L, rebalancingJobGroup.getJobs().get(3L).toBuilder().setWorkerId(2L).build());
              return null;
            })
        .when(rebalancer)
        .computeWorkerId(Mockito.any(), Mockito.any());

    jobManager.rebalanceJobGroups();

    // Verify that the correct data was written to ZK.
    ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> itemCaptor = ArgumentCaptor.forClass(Versioned.class);
    Mockito.verify(jobGroupStore, Mockito.times(1)).put(idCaptor.capture(), itemCaptor.capture());
    Assert.assertEquals(jobGroupKey, idCaptor.getValue());
    StoredJobGroup.Builder expectedJobGroupBuilder =
        StoredJobGroup.newBuilder().setJobGroup(expectedJobGroup);
    expectedJobGroupBuilder.addJobs(buildJob(1, 1, JobState.JOB_STATE_RUNNING, 0d));
    expectedJobGroupBuilder.addJobs(buildJob(2, 2, JobState.JOB_STATE_RUNNING, 0d));
    expectedJobGroupBuilder.addJobs(buildJob(3, 2, JobState.JOB_STATE_RUNNING, 0d));
    assertEqualsWithoutTimestamp(expectedJobGroupBuilder.build(), itemCaptor.getValue().model());
  }

  /** Verify that job group rebalances correctly when rebalancer returns new job configuration. */
  @Test
  public void testRebalanceJobGroupsWithJobConfigurationChange() throws Exception {
    Mockito.doAnswer(
            invocation -> {
              Map<String, RebalancingJobGroup> jobGroupMap = invocation.getArgument(0);
              Map<Long, StoredWorker> workerMap = invocation.getArgument(0);
              RebalancingJobGroup rebalancingJobGroup = jobGroupMap.get(jobGroupKey);
              StoredJob.Builder jobBuilder = rebalancingJobGroup.getJobs().get(2L).toBuilder();
              jobBuilder.getJobBuilder().getFlowControlBuilder().setMaxInflightMessages(10);
              rebalancingJobGroup.updateJob(2L, jobBuilder.build());
              return null;
            })
        .when(rebalancer)
        .computeJobConfiguration(Mockito.any(), Mockito.any());

    jobManager.rebalanceJobGroups();

    // Verify that the correct data was written to ZK.
    ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> itemCaptor = ArgumentCaptor.forClass(Versioned.class);
    Mockito.verify(jobGroupStore, Mockito.times(1)).put(idCaptor.capture(), itemCaptor.capture());
    Assert.assertEquals(jobGroupKey, idCaptor.getValue());
    StoredJobGroup.Builder expectedJobGroupBuilder = jobGroup.toBuilder();
    expectedJobGroupBuilder.clearJobs();
    expectedJobGroupBuilder.addJobs(buildJob(1L, 1L, JobState.JOB_STATE_RUNNING, 0d));
    StoredJob.Builder jobBuilder = buildJob(2L, 0L, JobState.JOB_STATE_RUNNING, 0d).toBuilder();
    jobBuilder.getJobBuilder().getFlowControlBuilder().setMaxInflightMessages(10);
    expectedJobGroupBuilder.addJobs(jobBuilder);
    // the worker id is 0 because we didn't change it.
    expectedJobGroupBuilder.addJobs(buildJob(3L, 0L, JobState.JOB_STATE_RUNNING, 0d));
    assertEqualsWithoutTimestamp(expectedJobGroupBuilder.build(), itemCaptor.getValue().model());
  }

  /** Verify that job group rebalances correctly when rebalancer returns new job state. */
  @Test
  public void testRebalanceJobGroupsWithJobStateChange() throws Exception {
    Mockito.doAnswer(
            invocation -> {
              Map<String, RebalancingJobGroup> jobGroupMap = invocation.getArgument(0);
              Map<Long, StoredWorker> workerMap = invocation.getArgument(0);
              RebalancingJobGroup rebalancingJobGroup = jobGroupMap.get(jobGroupKey);
              StoredJob.Builder jobBuilder = rebalancingJobGroup.getJobs().get(2L).toBuilder();
              jobBuilder.setState(JobState.JOB_STATE_CANCELED);
              rebalancingJobGroup.updateJob(2L, jobBuilder.build());
              return null;
            })
        .when(rebalancer)
        .computeJobConfiguration(Mockito.any(), Mockito.any());

    jobManager.rebalanceJobGroups();

    // Verify that the correct data was written to ZK.
    ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> itemCaptor = ArgumentCaptor.forClass(Versioned.class);
    Mockito.verify(jobGroupStore, Mockito.times(1)).put(idCaptor.capture(), itemCaptor.capture());
    Assert.assertEquals(jobGroupKey, idCaptor.getValue());
    StoredJobGroup.Builder expectedJobGroupBuilder =
        StoredJobGroup.newBuilder().setJobGroup(expectedJobGroup);
    expectedJobGroupBuilder.addJobs(buildJob(1, 1, JobState.JOB_STATE_RUNNING, 0d));
    expectedJobGroupBuilder.addJobs(buildJob(2, 0, JobState.JOB_STATE_CANCELED, 0d));
    // the worker id is 0 because we didn't change it.
    expectedJobGroupBuilder.addJobs(buildJob(3, 0, JobState.JOB_STATE_RUNNING, 0d));
    assertEqualsWithoutTimestamp(expectedJobGroupBuilder.build(), itemCaptor.getValue().model());
  }

  @Test
  public void testRebalanceJobGroupsWithScaleChange() throws Exception {
    double expectedScale = 3.0;
    Mockito.doAnswer(
            invocation -> {
              Map<String, RebalancingJobGroup> jobGroupMap = invocation.getArgument(0);
              RebalancingJobGroup rebalancingJobGroup = jobGroupMap.get(jobGroupKey);
              rebalancingJobGroup.updateScale(expectedScale);
              return jobGroupMap;
            })
        .when(rebalancer)
        .computeLoad(Mockito.any());

    jobManager.rebalanceJobGroups();

    // Verify that the correct data was written to ZK.
    ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Versioned<StoredJobGroup>> itemCaptor = ArgumentCaptor.forClass(Versioned.class);
    Mockito.verify(jobGroupStore, Mockito.times(1)).put(idCaptor.capture(), itemCaptor.capture());
    Assert.assertEquals(jobGroupKey, idCaptor.getValue());
    StoredJobGroup.Builder expectedJobGroupBuilder =
        StoredJobGroup.newBuilder().setJobGroup(expectedJobGroup);
    expectedJobGroupBuilder.addJobs(buildJob(1, 1, JobState.JOB_STATE_RUNNING, 0));
    expectedJobGroupBuilder.addJobs(buildJob(2, 0, JobState.JOB_STATE_RUNNING, 0));
    expectedJobGroupBuilder.addJobs(buildJob(3, 0, JobState.JOB_STATE_RUNNING, 0));
    expectedJobGroupBuilder.setScaleStatus(
        ScaleStatus.newBuilder().setScale(expectedScale).build());
    assertEqualsWithoutTimestamp(expectedJobGroupBuilder.build(), itemCaptor.getValue().model());
  }

  /**
   * Verify that rebalance job group is skipped when not the leader by checking that job group store
   * put has not been invoked.
   */
  @Test
  public void testRebalanceJobGroupsNotLeader() throws Exception {
    Mockito.when(leaderSelector.isLeader()).thenReturn(false);
    jobManager.rebalanceJobGroups();
    Mockito.verify(jobGroupStore, Mockito.times(0)).put(Mockito.anyString(), Mockito.any());
  }

  /** Verify that rebalance job group gracefully handles exception. */
  @Test
  public void testRebalanceJobGroupsWithException() throws Exception {
    Mockito.doThrow(new RuntimeException()).when(workerStore).getAll();
    jobManager.rebalanceJobGroups();
    Mockito.verify(jobGroupStore, Mockito.times(0)).put(Mockito.anyString(), Mockito.any());
  }

  /** Verify that log and metrics is skipped when not the leader. */
  @Test
  public void testLogAndMetricsNotLeader() throws Exception {
    Mockito.when(leaderSelector.isLeader()).thenReturn(false);
    jobManager.logAndMetrics();
  }

  /** Verify that log and metrics works. */
  @Test
  public void testLogAndMetrics() throws Exception {
    jobManager.logAndMetrics();
  }

  /** Verify that log and metrics handles exception. */
  @Test
  public void testLogAndMetricsWithException() throws Exception {
    Mockito.doThrow(new RuntimeException()).when(jobGroupStore).getAll();
    jobManager.logAndMetrics();
  }

  @Test
  public void shouldNotClearJobStatusWhenRebalancingJobGroupDoesNotChanged() throws Exception {
    Map<String, RebalancingJobGroup> rebalancingJobGroup =
        Collections.singletonMap(
            "test_topic_1",
            RebalancingJobGroup.of(
                Versioned.from(StoredJobGroup.newBuilder().build(), 1), ImmutableMap.of()));
    IdProvider<Long, StoredJobStatus> jobStatusIdProvider = new LocalSequencer<>();
    Store<Long, StoredJobStatus> jobStatusStore = new LocalStore<>(jobStatusIdProvider);

    jobManager =
        new JobManager(
            new NoopScope(),
            jobGroupStore,
            jobStatusStore,
            workerStore,
            rebalancer,
            shadowRebalancerDelegate,
            leaderSelector);

    StoredJobStatus status = StoredJobStatus.newBuilder().build();
    Long statusId = jobStatusIdProvider.getId(status);
    jobStatusStore.put(statusId, VersionedProto.from(status));

    jobManager.maybeClearJobStatuses(rebalancingJobGroup);
    Assert.assertEquals(1, jobStatusStore.getAll().size());
    Assert.assertEquals(status, jobStatusStore.get(statusId).model());
  }

  @Test
  public void shouldClearJobStatusWhenRebalancingJobGroupChanged() throws Exception {
    StoredJobGroup storedJobGroup =
        StoredJobGroup.newBuilder()
            .addJobs(
                StoredJob.newBuilder()
                    .setWorkerId(1)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .setJob(
                        Job.newBuilder()
                            .setJobId(1)
                            .setType(JobType.JOB_TYPE_LOAD_GEN_CONSUME)
                            .build())
                    .build())
            .build();
    Map<String, RebalancingJobGroup> rebalancingJobGroup =
        Collections.singletonMap(
            "test_topic_1",
            RebalancingJobGroup.of(Versioned.from(storedJobGroup, 1), ImmutableMap.of()));
    IdProvider<Long, StoredJobStatus> jobStatusIdProvider = new LocalSequencer<>();
    Store<Long, StoredJobStatus> jobStatusStore = new LocalStore<>(jobStatusIdProvider);

    jobManager =
        new JobManager(
            new NoopScope(),
            jobGroupStore,
            jobStatusStore,
            workerStore,
            rebalancer,
            shadowRebalancerDelegate,
            leaderSelector);

    StoredJobStatus status = StoredJobStatus.newBuilder().build();
    Long statusId = jobStatusIdProvider.getId(status);
    jobStatusStore.put(statusId, VersionedProto.from(status));
    Assert.assertEquals(1, jobStatusStore.getAll().size());

    // Update any one job
    final StoredJob job = rebalancingJobGroup.get("test_topic_1").getJobs().get(1L);
    final StoredJob updatedJob = job.toBuilder().setWorkerId(34).build();
    rebalancingJobGroup.get("test_topic_1").updateJob(job.getJob().getJobId(), updatedJob);

    jobManager.maybeClearJobStatuses(rebalancingJobGroup);
    Assert.assertTrue(jobStatusStore.getAll().isEmpty());
  }

  /** Assert that the two stored job groups are the same except for the last updated timestamp. */
  private void assertEqualsWithoutTimestamp(StoredJobGroup expected, StoredJobGroup actual) {
    StoredJobGroup.Builder expectedBuilder = StoredJobGroup.newBuilder(expected);
    expectedBuilder.clearLastUpdated();
    expectedBuilder.getJobsBuilderList().forEach(j -> j.clearLastUpdated());

    StoredJobGroup.Builder actualBuilder = StoredJobGroup.newBuilder(actual);
    actualBuilder.clearLastUpdated();
    actualBuilder.getJobsBuilderList().forEach(j -> j.clearLastUpdated());

    Assert.assertEquals(expectedBuilder.build(), actualBuilder.build());
  }
}
