package com.uber.data.kafka.datatransfer.controller.creator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.AdminClient;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.JobUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
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

  private AdminClient.Builder adminBuilder;
  private AdminClient adminClient;
  private BatchJobCreator jobCreator;
  private StoredJobGroup storedJobGroup;
  private KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> listConsumerOffsetFutureMock;
  private ListConsumerGroupOffsetsResult listConsumerGroupOffsetsResult;

  @Before
  public void setUp() {
    adminClient = Mockito.mock(AdminClient.class);
    adminBuilder = Mockito.mock(AdminClient.Builder.class);
    Mockito.doReturn(adminClient).when(adminBuilder).build(Mockito.anyString());
    jobCreator = new BatchJobCreator(adminBuilder, CoreInfra.NOOP);
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
  public void testNewJobWhenThePartitionIsEmpty() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    KafkaFutureImpl offsetFuture = new KafkaFutureImpl();
    offsetFuture.complete(
        new ListOffsetsResult.ListOffsetsResultInfo(TEST_END_OFFSET, 0, Optional.empty()));
    Mockito.when(adminClient.beginningOffsets(ImmutableList.of(topicPartition)))
        .thenReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, offsetFuture)));
    Mockito.when(adminClient.endOffsets(ImmutableList.of(topicPartition)))
        .thenReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, offsetFuture)));
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
        .when(adminClient)
        .listConsumerGroupOffsets(TEST_GROUP);
    KafkaFutureImpl startOffsetFuture = new KafkaFutureImpl();
    startOffsetFuture.complete(
        new ListOffsetsResult.ListOffsetsResultInfo(
            TEST_START_OFFSET, Timestamps.toMillis(TEST_START_TIME), Optional.empty()));
    KafkaFutureImpl endOffsetFuture = new KafkaFutureImpl();
    endOffsetFuture.complete(
        new ListOffsetsResult.ListOffsetsResultInfo(
            TEST_END_OFFSET, Timestamps.toMillis(TEST_END_TIME), Optional.empty()));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, startOffsetFuture)))
        .when(adminClient)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_START_TIME)));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, startOffsetFuture)))
        .when(adminClient)
        .beginningOffsets(ImmutableList.of(topicPartition));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, endOffsetFuture)))
        .when(adminClient)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_END_TIME)));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, endOffsetFuture)))
        .when(adminClient)
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
        .when(adminClient)
        .listConsumerGroupOffsets(TEST_GROUP);
    KafkaFutureImpl endOffsetFuture = new KafkaFutureImpl();
    endOffsetFuture.complete(
        new ListOffsetsResult.ListOffsetsResultInfo(
            TEST_END_OFFSET, Timestamps.toMillis(TEST_END_TIME), Optional.empty()));
    Mockito.doReturn(new ListOffsetsResult(Collections.emptyMap()))
        .when(adminClient)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_END_TIME)));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, endOffsetFuture)))
        .when(adminClient)
        .endOffsets(ImmutableList.of(topicPartition));
    StoredJob storedJob = jobCreator.newJob(storedJobGroup, 1, TEST_PARTITION);
    Assert.assertTrue(JobUtils.isDerived(storedJobGroup.getJobGroup(), storedJob.getJob()));
    Assert.assertEquals(JobState.JOB_STATE_CANCELED, storedJob.getState());
  }

  // different start and end timestamp map to the same offsets.
  @Test
  public void testNewJobWithSameStartAndEndOffsets() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    KafkaFutureImpl endOffsetFuture = new KafkaFutureImpl();
    endOffsetFuture.complete(
        new ListOffsetsResult.ListOffsetsResultInfo(
            TEST_END_OFFSET, Timestamps.toMillis(TEST_END_TIME), Optional.empty()));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, endOffsetFuture)))
        .when(adminClient)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_START_TIME)));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, endOffsetFuture)))
        .when(adminClient)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_END_TIME)));
    StoredJob storedJob = jobCreator.newJob(storedJobGroup, 1, TEST_PARTITION);
    Assert.assertTrue(JobUtils.isDerived(storedJobGroup.getJobGroup(), storedJob.getJob()));
    Assert.assertEquals(JobState.JOB_STATE_CANCELED, storedJob.getState());
  }

  @Test
  public void testOffsetForTimesThrowsExceptionForEndOffsets() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);

    // High watermark is available
    KafkaFutureImpl highWatermarkFuture = new KafkaFutureImpl();
    highWatermarkFuture.complete(
        new ListOffsetsResult.ListOffsetsResultInfo(
            TEST_END_OFFSET, Timestamps.toMillis(TEST_END_TIME), Optional.empty()));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, highWatermarkFuture)))
        .when(adminClient)
        .endOffsets(ImmutableList.of(topicPartition));
    // offsetForTimes throws exception
    KafkaFutureImpl endOffsetFuture = new KafkaFutureImpl();
    endOffsetFuture.completeExceptionally(new TimeoutException("time out translating offset"));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, endOffsetFuture)))
        .when(adminClient)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_END_TIME)));

    StoredJob storedJob = jobCreator.newJob(storedJobGroup, 1, TEST_PARTITION);
    Assert.assertTrue(JobUtils.isDerived(storedJobGroup.getJobGroup(), storedJob.getJob()));
    Assert.assertEquals(TEST_END_OFFSET, storedJob.getJob().getKafkaConsumerTask().getEndOffset());
  }

  @Test
  public void testOffsetForTimesThrowsExceptionForStartOffsets() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);

    // High-watermark and End offset are available
    KafkaFutureImpl endOffsetFuture = new KafkaFutureImpl();
    endOffsetFuture.complete(
        new ListOffsetsResult.ListOffsetsResultInfo(
            TEST_END_OFFSET, Timestamps.toMillis(TEST_END_TIME), Optional.empty()));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, endOffsetFuture)))
        .when(adminClient)
        .endOffsets(ImmutableList.of(topicPartition));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, endOffsetFuture)))
        .when(adminClient)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_END_TIME)));
    // offsetForTimes throws exception
    KafkaFutureImpl startOffsetFuture = new KafkaFutureImpl();
    startOffsetFuture.completeExceptionally(new TimeoutException("time out translating offset"));
    Mockito.doReturn(new ListOffsetsResult(ImmutableMap.of(topicPartition, startOffsetFuture)))
        .when(adminClient)
        .offsetsForTimes(ImmutableMap.of(topicPartition, Timestamps.toMillis(TEST_START_TIME)));

    StoredJob storedJob = jobCreator.newJob(storedJobGroup, 1, TEST_PARTITION);
    Assert.assertTrue(JobUtils.isDerived(storedJobGroup.getJobGroup(), storedJob.getJob()));
    Assert.assertEquals(
        TEST_END_OFFSET, storedJob.getJob().getKafkaConsumerTask().getStartOffset());
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
    Assert.assertEquals(
        TEST_START_OFFSET,
        BatchJobCreator.getOffset(() -> TEST_START_OFFSET, () -> TEST_START_OFFSET));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetOffsetNullOffset() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    BatchJobCreator.getOffset(
        () -> null,
        () -> {
          throw new IllegalArgumentException(
              String.format("failed to resolve offsetForTimes for %s", topicPartition));
        });
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetOffsetNegativeOffset() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, TEST_PARTITION);
    BatchJobCreator.getOffset(
        () -> -1L,
        () -> {
          throw new IllegalArgumentException(
              String.format("failed to resolve offsetForTimes for %s", topicPartition));
        });
  }
}
