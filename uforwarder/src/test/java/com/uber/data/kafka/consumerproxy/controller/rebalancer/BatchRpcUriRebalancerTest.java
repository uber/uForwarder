package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.common.AdminClient;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import com.uber.data.kafka.datatransfer.controller.autoscalar.AutoScalar;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.m3.tally.NoopScope;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class BatchRpcUriRebalancerTest {
  private static final String CLUSTER = "cluster";
  private static final String CONSUMER_GROUP = "consumer_group";
  private static final String TOPIC = "topic";
  private static final String Job_GROUP = "jobGroup";
  private static final int PARTITION = 2;
  private BatchRpcUriRebalancer rebalancer;
  private AutoScalar autoScalar;
  private HibernatingJobRebalancer hibernatingJobRebalancer;
  private AdminClient.Builder adminClientBuilder;

  @BeforeEach
  public void setup() throws IOException {
    RebalancerConfiguration config = new RebalancerConfiguration();
    adminClientBuilder = Mockito.mock(AdminClient.Builder.class);
    autoScalar = Mockito.mock(AutoScalar.class);
    hibernatingJobRebalancer = Mockito.mock(HibernatingJobRebalancer.class);
    DynamicConfiguration dynamicConfiguration = Mockito.mock(DynamicConfiguration.class);
    Mockito.when(dynamicConfiguration.isOffsetCommittingEnabled()).thenReturn(true);
    Mockito.doAnswer(invocation -> Collections.EMPTY_SET)
        .when(hibernatingJobRebalancer)
        .computeWorkerId(Mockito.anyList(), Mockito.anyMap());
    rebalancer =
        new BatchRpcUriRebalancer(
            new NoopScope(),
            config,
            autoScalar,
            hibernatingJobRebalancer,
            adminClientBuilder,
            dynamicConfiguration);
  }

  @Test
  public void testComputeJobStateCancelJobGroupWithSameStartEndTimestamp() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_RUNNING, 2, 2, 2, 2, false);
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertTrue(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(
        JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobs().get(1L).getState());
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testComputeJobStateCancelJobGroupWithCommittedOffsetReachingEndOffset()
      throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_RUNNING, 0, 0, 2, 2, false);
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertTrue(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(
        JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobs().get(1L).getState());
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testComputeJobStateDoNotRecancelCanceledJob() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1, JobState.JOB_STATE_CANCELED, JobState.JOB_STATE_CANCELED, 2, 0, 2, 2, false);
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertFalse(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(
        JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobs().get(1L).getState());
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testComputeJobStateCancelAtEndOffset() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_RUNNING, 2, 0, 2, 2, false);
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertTrue(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(
        JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobs().get(1L).getState());
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testComputeJobStateCancelAfterEndOffset() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_RUNNING, 2, 0, 2, 3, false);
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertTrue(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(
        JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobs().get(1L).getState());
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testComputeJobStateInvalidToRunning() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1L, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_INVALID, 2, 0, 2, 0, false);
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertTrue(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(
        JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobs().get(1L).getState());
    Assertions.assertEquals(JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testComputeJobStateRunningToRunning() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1L, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_RUNNING, 2, 0, 2, 0, false);
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertFalse(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(
        JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobs().get(1L).getState());
    Assertions.assertEquals(JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testComputeJobStateCanceledJobCanelsJobGroup() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1L, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_CANCELED, 2, 0, 2, 3, false);
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertTrue(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(
        JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobs().get(1L).getState());
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobGroupState());
  }

  /**
   * This represents the starting state of a DLQ merge job where a job group with no jobs is added
   */
  @Test
  public void testComputeJobStateDoesNotCancelWhenNoJobs() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            -1, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_CANCELED, 2, 0, 2, 3, false);
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertFalse(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testPostProcessNoCommitDueToTime() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_RUNNING, 0, 0, 2, 2, false);
    rebalancer.postProcess(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Mockito.verify(adminClientBuilder, Mockito.never()).build(Mockito.anyString());
  }

  @Test
  public void testPostProcessNoCommitDueToZeroEndOffset() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1,
            JobState.JOB_STATE_RUNNING,
            JobState.JOB_STATE_RUNNING,
            System.currentTimeMillis(),
            0,
            0,
            2,
            false);
    rebalancer.postProcess(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Mockito.verify(adminClientBuilder, Mockito.never()).build(Mockito.anyString());
  }

  @Test
  public void testPostProcessNoCommitDueToDifferentOffsets() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1,
            JobState.JOB_STATE_RUNNING,
            JobState.JOB_STATE_RUNNING,
            System.currentTimeMillis(),
            1,
            2,
            2,
            false);
    rebalancer.postProcess(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Mockito.verify(adminClientBuilder, Mockito.never()).build(Mockito.anyString());
  }

  @Test
  public void testPostProcessNoCommitDueToJobRunning() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1,
            JobState.JOB_STATE_RUNNING,
            JobState.JOB_STATE_RUNNING,
            System.currentTimeMillis(),
            2,
            2,
            2,
            false);
    rebalancer.postProcess(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Mockito.verify(adminClientBuilder, Mockito.never()).build(Mockito.anyString());
  }

  @Test
  public void testPostProcessNoCommitWithFlipr() throws Exception {
    DynamicConfiguration dynamicConfiguration = Mockito.mock(DynamicConfiguration.class);
    Mockito.when(dynamicConfiguration.isOffsetCommittingEnabled()).thenReturn(false);
    rebalancer =
        new BatchRpcUriRebalancer(
            new NoopScope(),
            new RebalancerConfiguration(),
            autoScalar,
            hibernatingJobRebalancer,
            adminClientBuilder,
            dynamicConfiguration);

    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1,
            JobState.JOB_STATE_RUNNING,
            JobState.JOB_STATE_CANCELED,
            System.currentTimeMillis(),
            2,
            2,
            2,
            true);
    rebalancer.postProcess(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());

    Mockito.verify(adminClientBuilder, Mockito.never()).build(Mockito.anyString());
  }

  @Test
  public void testPostProcessNoCommitWithCommit() throws Exception {
    AdminClient adminClient = Mockito.mock(AdminClient.class);
    Mockito.when(adminClientBuilder.build(CLUSTER)).thenReturn(adminClient);

    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            1,
            JobState.JOB_STATE_RUNNING,
            JobState.JOB_STATE_CANCELED,
            System.currentTimeMillis(),
            2,
            2,
            2,
            true);
    rebalancer.postProcess(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    ArgumentCaptor<String> clusterCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> consumerGroupCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> mapCaptor =
        ArgumentCaptor.forClass(Map.class);
    Mockito.verify(adminClientBuilder, Mockito.times(1)).build(clusterCaptor.capture());
    Mockito.verify(adminClient, Mockito.times(1))
        .alterConsumerGroupOffsets(consumerGroupCaptor.capture(), mapCaptor.capture());
    Assertions.assertEquals(CLUSTER, clusterCaptor.getValue());
    Assertions.assertEquals(CONSUMER_GROUP, consumerGroupCaptor.getValue());
    Map<TopicPartition, OffsetAndMetadata> partitionAndOffsetToCommit = mapCaptor.getValue();
    Assertions.assertEquals(1, partitionAndOffsetToCommit.size());
    Assertions.assertEquals(
        2, partitionAndOffsetToCommit.get(new TopicPartition(TOPIC, PARTITION)).offset());
  }

  private RebalancingJobGroup buildRebalancingJobGroup(
      long jobId,
      JobState jobGroupState,
      JobState expected,
      long endTimestamp,
      long startOffset,
      long endOffset,
      long commitOffset,
      boolean isSecure) {
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.setState(jobGroupState);
    if (jobId >= 0) {
      StoredJob.Builder jobBuilder = StoredJob.newBuilder();
      jobBuilder.getJobBuilder().setJobId(jobId);
      jobBuilder.setState(expected);
      jobBuilder
          .getJobBuilder()
          .getKafkaConsumerTaskBuilder()
          .setPartition(PARTITION)
          .setStartOffset(startOffset)
          .setEndOffset(endOffset);
      StoredJob job = jobBuilder.build();
      jobGroupBuilder.addJobs(job);
    }
    jobGroupBuilder
        .getJobGroupBuilder()
        .getKafkaConsumerTaskGroupBuilder()
        .setCluster(CLUSTER)
        .setConsumerGroup(CONSUMER_GROUP)
        .setTopic(TOPIC)
        .setEndTimestamp(Timestamps.fromMillis(endTimestamp));

    jobGroupBuilder.getJobGroupBuilder().getSecurityConfigBuilder().setIsSecure(isSecure);

    StoredJobGroup jobGroup = jobGroupBuilder.build();
    StoredJobStatus.Builder jobStatusBuilder = StoredJobStatus.newBuilder();
    jobStatusBuilder
        .getJobStatusBuilder()
        .getKafkaConsumerTaskStatusBuilder()
        .setCommitOffset(commitOffset);
    StoredJobStatus jobStatus = jobStatusBuilder.build();
    return RebalancingJobGroup.of(Versioned.from(jobGroup, 0), ImmutableMap.of(1L, jobStatus));
  }

  @Test
  public void testPartitionOffsetsPreventTimestampCancellation() throws Exception {
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.setState(JobState.JOB_STATE_RUNNING);
    jobGroupBuilder
        .getJobGroupBuilder()
        .setJobGroupId(Job_GROUP)
        .getKafkaConsumerTaskGroupBuilder()
        .setCluster(CLUSTER)
        .setTopic(TOPIC)
        .setConsumerGroup(CONSUMER_GROUP)
        .setStartTimestamp(Timestamps.fromMillis(1000))
        .setEndTimestamp(Timestamps.fromMillis(1000))
        .getPartitionOffsetRangesBuilder()
        .addPartitionOffsetRange(
            com.uber.data.kafka.datatransfer.PartitionOffsetRange.newBuilder()
                .setPartition(PARTITION)
                .setStartOffset(100)
                .setEndOffset(200)
                .build());

    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    jobBuilder.setState(JobState.JOB_STATE_RUNNING);
    jobBuilder
        .getJobBuilder()
        .setJobId(1)
        .getKafkaConsumerTaskBuilder()
        .setCluster(CLUSTER)
        .setTopic(TOPIC)
        .setPartition(PARTITION)
        .setConsumerGroup(CONSUMER_GROUP)
        .setStartOffset(100)
        .setEndOffset(200);
    jobGroupBuilder.addJobs(jobBuilder.build());

    RebalancingJobGroup rebalancingJobGroup =
        RebalancingJobGroup.of(Versioned.from(jobGroupBuilder.build(), 0), ImmutableMap.of());

    // This should NOT cancel the job group because partition offsets are provided
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertEquals(JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobGroupState());
    Assertions.assertEquals(
        JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobs().get(1L).getState());
  }

  @Test
  public void testEqualTimestampsWithoutPartitionOffsetsCauseCancellation() throws Exception {
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.setState(JobState.JOB_STATE_RUNNING);
    jobGroupBuilder
        .getJobGroupBuilder()
        .setJobGroupId(Job_GROUP)
        .getKafkaConsumerTaskGroupBuilder()
        .setCluster(CLUSTER)
        .setTopic(TOPIC)
        .setConsumerGroup(CONSUMER_GROUP)
        .setStartTimestamp(Timestamps.fromMillis(1000))
        .setEndTimestamp(Timestamps.fromMillis(1000));

    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    jobBuilder.setState(JobState.JOB_STATE_RUNNING);
    jobBuilder
        .getJobBuilder()
        .setJobId(1)
        .getKafkaConsumerTaskBuilder()
        .setCluster(CLUSTER)
        .setTopic(TOPIC)
        .setPartition(PARTITION)
        .setConsumerGroup(CONSUMER_GROUP)
        .setStartOffset(100)
        .setEndOffset(200);
    jobGroupBuilder.addJobs(jobBuilder.build());

    RebalancingJobGroup rebalancingJobGroup =
        RebalancingJobGroup.of(Versioned.from(jobGroupBuilder.build(), 0), ImmutableMap.of());

    // This SHOULD cancel the job group because timestamps are equal and no partition offsets
    rebalancer.computeJobState(ImmutableMap.of(Job_GROUP, rebalancingJobGroup), ImmutableMap.of());
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobGroupState());
  }
}
