package com.uber.data.kafka.datatransfer.controller.creator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.uber.data.kafka.clients.admin.Admin;
import com.uber.data.kafka.clients.admin.MultiClusterAdmin;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.JobUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BatchJobCreatorTest extends FievelTestBase {
  private static final String TEST_CLUSTER = "test-cluster";
  private static final String TEST_GROUP = "test-group";
  private static final String TEST_TOPIC = "test-topic";
  private static final int TEST_PARTITION = 5;
  private static final Timestamp TEST_START_TIME = Timestamps.fromSeconds(1);
  private static final long TEST_START_OFFSET = 3L;
  private static final Timestamp TEST_END_TIME = Timestamps.fromSeconds(2);
  private static final long TEST_END_OFFSET = 4L;

  private MultiClusterAdmin multiClusterAdmin;
  private Admin singleClusterAdmin;
  private BatchJobCreator jobCreator;
  private StoredJobGroup storedJobGroup;
  private KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> listConsumerOffsetFutureMock;
  private ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult;

  @Before
  public void setUp() {
    singleClusterAdmin = Mockito.mock(Admin.class);
    multiClusterAdmin = Mockito.mock(MultiClusterAdmin.class);
    Mockito.doReturn(singleClusterAdmin).when(multiClusterAdmin).getAdmin(Mockito.anyString());
    jobCreator = new BatchJobCreator(multiClusterAdmin, CoreInfra.NOOP);
    storedJobGroup =
        StoredJobGroup.newBuilder()
            .setJobGroup(
                JobGroup.newBuilder()
                    .setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER)
                    .setKafkaConsumerTaskGroup(
                        KafkaConsumerTaskGroup.newBuilder()
                            .setCluster(TEST_CLUSTER)
                            .setConsumerGroup(TEST_GROUP)
                            .setTopic(TEST_TOPIC)
                            .setStartTimestamp(TEST_START_TIME)
                            .setEndTimestamp(TEST_END_TIME)
                            .build()))
            .setState(JobState.JOB_STATE_RUNNING)
            .build();
    listConsumerOffsetFutureMock = Mockito.mock(KafkaFuture.class);
    listConsumerGroupOffsetsResult = Mockito.mock(ListConsumerGroupOffsetsResult.class);
    Mockito.doReturn(listConsumerOffsetFutureMock)
        .when(listConsumerGroupOffsetsResult)
        .partitionsToOffsetAndMetadata();
  }

  @Test
  public void testNewJobWhenThePartitionIsEmpty()
      throws ExecutionException, InterruptedException, TimeoutException {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    Mockito.when(singleClusterAdmin.beginningOffsets(ImmutableList.of(topicPartition)))
        .thenReturn(ImmutableMap.of(topicPartition, TEST_END_OFFSET));
    Mockito.when(singleClusterAdmin.endOffsets(ImmutableList.of(topicPartition)))
        .thenReturn(ImmutableMap.of(topicPartition, TEST_END_OFFSET));
    StoredJob storedJob = jobCreator.newJob(storedJobGroup, 1, TEST_PARTITION);
    Assert.assertTrue(JobUtils.isDerived(storedJobGroup.getJobGroup(), storedJob.getJob()));
    Assert.assertEquals(JobState.JOB_STATE_CANCELED, storedJob.getState());
  }

  @Test
  public void testNewJobWithDifferentStartAndEndOffsets()
      throws ExecutionException, InterruptedException, TimeoutException {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    Mockito.doReturn(ImmutableMap.of(topicPartition, new OffsetAndMetadata(TEST_START_OFFSET)))
        .when(listConsumerOffsetFutureMock)
        .get(20000, TimeUnit.MILLISECONDS);
    Mockito.doReturn(listConsumerGroupOffsetsResult)
        .when(singleClusterAdmin)
        .listConsumerGroupOffsets(TEST_GROUP);
    Mockito.doReturn(
            ImmutableMap.of(
                topicPartition,
                new OffsetAndTimestamp(TEST_START_OFFSET, Timestamps.toMillis(TEST_START_TIME))))
        .when(singleClusterAdmin)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_START_TIME)));
    Mockito.doReturn(ImmutableMap.of(topicPartition, TEST_START_OFFSET))
        .when(singleClusterAdmin)
        .endOffsets(ImmutableList.of(topicPartition));
    Mockito.doReturn(
            ImmutableMap.of(
                topicPartition,
                new OffsetAndTimestamp(TEST_END_OFFSET, Timestamps.toMillis(TEST_END_TIME))))
        .when(singleClusterAdmin)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_END_TIME)));
    Mockito.doReturn(ImmutableMap.of(topicPartition, TEST_END_OFFSET))
        .when(singleClusterAdmin)
        .endOffsets(ImmutableList.of(topicPartition));
    StoredJob storedJob = jobCreator.newJob(storedJobGroup, 1, TEST_PARTITION);
    Assert.assertTrue(JobUtils.isDerived(storedJobGroup.getJobGroup(), storedJob.getJob()));
    Assert.assertEquals(JobState.JOB_STATE_RUNNING, storedJob.getState());
  }

  @Test
  public void testNewJobWithTooLargeStartAndEndTimestamp()
      throws ExecutionException, InterruptedException, TimeoutException {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    Mockito.doReturn(ImmutableMap.of(topicPartition, new OffsetAndMetadata(TEST_END_OFFSET)))
        .when(listConsumerOffsetFutureMock)
        .get(20000, TimeUnit.MILLISECONDS);
    Mockito.doReturn(listConsumerGroupOffsetsResult)
        .when(singleClusterAdmin)
        .listConsumerGroupOffsets(TEST_GROUP);
    Mockito.doReturn(ImmutableMap.of())
        .when(singleClusterAdmin)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_END_TIME)));
    Mockito.doReturn(ImmutableMap.of(topicPartition, TEST_END_OFFSET))
        .when(singleClusterAdmin)
        .endOffsets(ImmutableList.of(topicPartition));
    StoredJob storedJob = jobCreator.newJob(storedJobGroup, 1, TEST_PARTITION);
    Assert.assertTrue(JobUtils.isDerived(storedJobGroup.getJobGroup(), storedJob.getJob()));
    Assert.assertEquals(JobState.JOB_STATE_CANCELED, storedJob.getState());
  }

  // different start and end timestamp map to the same offsets.
  @Test
  public void testNewJobWithSameStartAndEndOffsets() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    Mockito.doReturn(
            ImmutableMap.of(
                topicPartition,
                new OffsetAndTimestamp(TEST_END_OFFSET, Timestamps.toMillis(TEST_END_TIME))))
        .when(singleClusterAdmin)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_START_TIME)));
    Mockito.doReturn(
            ImmutableMap.of(
                topicPartition,
                new OffsetAndTimestamp(TEST_END_OFFSET, Timestamps.toMillis(TEST_END_TIME))))
        .when(singleClusterAdmin)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_END_TIME)));
    StoredJob storedJob = jobCreator.newJob(storedJobGroup, 1, TEST_PARTITION);
    Assert.assertTrue(JobUtils.isDerived(storedJobGroup.getJobGroup(), storedJob.getJob()));
    Assert.assertEquals(JobState.JOB_STATE_CANCELED, storedJob.getState());
  }

  @Test
  public void testAssertValidOffsets() {
    BatchJobCreator.assertValidOffsets(1, 2);
  }

  @Test
  public void testAssertValidOffsetsSameOffset() {
    BatchJobCreator.assertValidOffsets(2, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAssertValidOffsetsLargerStartOffset() {
    BatchJobCreator.assertValidOffsets(3, 2);
  }

  @Test
  public void testAssertValidTimestamps() {
    BatchJobCreator.assertValidTimestamps(1, 2);
  }

  @Test
  public void testAssertValidTimestampZero() {
    BatchJobCreator.assertValidTimestamps(0, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAssertValidTimestampNegativeStartTime() {
    BatchJobCreator.assertValidTimestamps(-1, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAssertValidTimestampNegativeEndTime() {
    BatchJobCreator.assertValidTimestamps(1, -1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAssertValidTimestampsTooLargeEndTime() {
    BatchJobCreator.assertValidTimestamps(2, Long.MAX_VALUE);
  }

  @Test
  public void testAssertValidTimestampsSameStart() {
    BatchJobCreator.assertValidTimestamps(2, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAssertValidTimestampsLargerStartTimestamp() {
    BatchJobCreator.assertValidTimestamps(3, 2);
  }

  @Test
  public void testGetOffset() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    Assert.assertEquals(
        TEST_START_OFFSET,
        BatchJobCreator.getOffset(
            ImmutableMap.of(
                topicPartition,
                new OffsetAndTimestamp(TEST_START_OFFSET, Timestamps.toMillis(TEST_START_TIME))),
            topicPartition,
            () -> TEST_START_OFFSET));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetOffsetNullOffset() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    BatchJobCreator.getOffset(
        ImmutableMap.of(),
        topicPartition,
        () -> {
          throw new IllegalArgumentException(
              String.format("failed to resolve offsetForTimes for %s", topicPartition));
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetOffsetNegativeOffset() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    BatchJobCreator.getOffset(
        ImmutableMap.of(
            topicPartition, new OffsetAndTimestamp(-1, Timestamps.toMillis(TEST_START_TIME))),
        topicPartition,
        () -> {
          throw new IllegalArgumentException(
              String.format("failed to resolve offsetForTimes for %s", topicPartition));
        });
  }
}
