package com.uber.data.kafka.datatransfer.controller.creator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.PartitionOffsetRange;
import com.uber.data.kafka.datatransfer.PartitionOffsetRanges;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.AdminClient;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchJobCreator creates a StoredJob using the given StoredJobGroup as a template,
 *
 * <ol>
 *   <li>sets jobId and partition to the given values.
 *   <li>sets start offset to the low watermark and end offset to the high watermark for the kafka
 *       consumer.
 * </ol>
 */
public class BatchJobCreator extends JobCreatorWithOffsets {
  private static final Logger logger = LoggerFactory.getLogger(BatchJobCreator.class);
  private static final Duration KAFKA_ADMIN_CLIENT_LIST_OFFSETS_TIMEOUT = Duration.ofMinutes(1);
  private static final String JOB_TYPE = "batch";

  private final AdminClient.Builder adminBuilder;
  private final Scope scope;
  private final CoreInfra infra;

  public BatchJobCreator(AdminClient.Builder adminBuilder, CoreInfra infra) {
    this.adminBuilder = adminBuilder;
    this.scope = infra.scope().tagged(ImmutableMap.of("mode", "batch"));
    this.infra = infra;
  }

  @Override
  public StoredJob newJob(StoredJobGroup storedJobGroup, long jobId, int partition) {
    final KafkaConsumerTaskGroup consumerTaskGroup =
        storedJobGroup.getJobGroup().getKafkaConsumerTaskGroup();
    final String cluster = consumerTaskGroup.getCluster();
    final String topic = consumerTaskGroup.getTopic();
    final String consumerGroup = consumerTaskGroup.getConsumerGroup();
    final TopicPartition topicPartition = new TopicPartition(topic, partition);
    return Instrumentation.instrument.withRuntimeException(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          AdminClient adminClient = adminBuilder.build(cluster);
          OffsetRange partitionWatermarks =
              getPartitionLowAndHighWatermarks(topicPartition, consumerGroup, adminClient);

          // there are no messages in this partition
          if (partitionWatermarks.start() == partitionWatermarks.end()) {
            return newJob(
                scope, logger, storedJobGroup, JOB_TYPE, jobId, partition, partitionWatermarks);
          }

          // Use partition offsets if available
          if (!consumerTaskGroup
              .getPartitionOffsetRanges()
              .getPartitionOffsetRangeList()
              .isEmpty()) {
            logger.info(
                "Using partition offsets for job creation",
                StructuredLogging.kafkaTopic(topic),
                StructuredLogging.kafkaGroup(consumerGroup),
                StructuredLogging.kafkaPartition(partition));
            return newJob(
                scope,
                logger,
                storedJobGroup,
                JOB_TYPE,
                jobId,
                partition,
                getStartEndOffsetsFromPartitionOffsets(
                    consumerTaskGroup.getPartitionOffsetRanges(), partition));
          }

          // Use the timestamps
          return newJob(
              scope,
              logger,
              storedJobGroup,
              JOB_TYPE,
              jobId,
              partition,
              getStartEndOffsetsFromTimestamp(
                  consumerTaskGroup, partition, partitionWatermarks.end(), adminClient));
        },
        "creator.job.create",
        StructuredFields.KAFKA_CLUSTER,
        cluster,
        StructuredFields.KAFKA_TOPIC,
        topic,
        StructuredFields.KAFKA_PARTITION,
        Integer.toString(partition));
  }

  private OffsetRange getPartitionLowAndHighWatermarks(
      TopicPartition topicPartition, String consumerGroup, AdminClient adminClient) {
    Stopwatch beginningOffsetLatencyWatch =
        scope.timer(MetricNames.BEGINNING_OFFSETS_LATENCY).start();
    long lowWatermark =
        offsetOf(
            adminClient.beginningOffsets(ImmutableList.of(topicPartition)),
            topicPartition,
            consumerGroup,
            0,
            beginningOffsetLatencyWatch);
    Stopwatch endOffsetLatencyWatch = scope.timer(MetricNames.END_OFFSETS_LATENCY).start();
    long highWatermark =
        offsetOf(
            adminClient.endOffsets(ImmutableList.of(topicPartition)),
            topicPartition,
            consumerGroup,
            0,
            endOffsetLatencyWatch);
    return new OffsetRange(lowWatermark, highWatermark);
  }

  private OffsetRange getStartEndOffsetsFromPartitionOffsets(
      PartitionOffsetRanges partitionOffsetRanges, int partition) {
    Optional<PartitionOffsetRange> partitionOffsetRange =
        partitionOffsetRanges.getPartitionOffsetRangeList().stream()
            .filter(range -> range.getPartition() == partition)
            .findFirst();
    return partitionOffsetRange
        .map(
            offsetRange ->
                new OffsetRange(offsetRange.getStartOffset(), offsetRange.getEndOffset()))
        .orElse(new OffsetRange(0, 0));
  }

  private OffsetRange getStartEndOffsetsFromTimestamp(
      KafkaConsumerTaskGroup consumerTaskGroup,
      int partition,
      long highWatermark,
      AdminClient adminClient) {
    String topic = consumerTaskGroup.getTopic();
    String consumerGroup = consumerTaskGroup.getConsumerGroup();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    long startTimestamp = Timestamps.toMillis(consumerTaskGroup.getStartTimestamp());
    long endTimestamp = Timestamps.toMillis(consumerTaskGroup.getEndTimestamp());
    assertValidTimestamps(startTimestamp, endTimestamp);

    // offsetForTimes returns null or -1 in the following cases:
    // 1. query timestamp > highwatermark timestamp
    // 2. topic partition has no data (either due to retention or lack of use).
    //
    // If there is at least one message in the topic partition, the following are true:
    // 1. query timestamp < lowwatermark timestamp
    // 2. query offset within the range returns the first message with timestamp > queried
    // timestamp.
    //
    // NOTE: this only works for kafka-client >= 2.2.1. This does NOT work correctly for kafka
    // 1.1.1 client due to some bugs.
    //
    // Test result when querying timestamp 1 (1 ms after epoch) and 1646259569000 (March 2022)
    // in March 2020:
    //
    // lowwatermark = 96823354
    // highwatermark = 96823388
    //
    // bash% ./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list
    // localhost:9092 --topic topic_1
    // --partitions 0 --time 1
    // topic_1:0:96823354
    // bash% ./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list
    // localhost:9092 --topic topic_1
    // --partitions 0 --time 1646259569000
    // topic_1:0:
    //
    // lowwatermark = 0
    // highwatermark = 0
    //
    // bash% ./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list
    // localhost:9092 --topic topic_2
    // --partitions 0 --time 1
    // topic_2:0:
    // bash% ./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list
    // localhost:9092 --topic topic_3
    // --partitions 0 --time 1646259569000
    // topic_3:0:
    //
    // lowwatermark =  221
    // highwatermark = 221
    //
    // bash% ./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list
    // localhost:9092 --topic
    // topic_4 --partitions 0 --time 1
    // topic_4:0:
    // bash% ./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list
    // localhost:9092 --topic
    // topic_4 --partitions 0 --time 1646259569000
    // topic_4:0:

    long endOffset =
        getOffset(
            () -> {
              Stopwatch watch = scope.timer(MetricNames.OFFSET_FOR_TIMES_LATENCY).start();
              return offsetOf(
                  adminClient.offsetsForTimes(ImmutableMap.of(topicPartition, endTimestamp)),
                  topicPartition,
                  consumerGroup,
                  -1,
                  watch);
            },
            () -> {
              // offsetForTimes may return null result of querying timestamp >
              // highwatermark timestamp, fallback endOffset in this case.
              logger.warn(
                  "failed to get end offset, falling back to high watermark",
                  StructuredLogging.kafkaTopic(topic),
                  StructuredLogging.kafkaGroup(consumerGroup),
                  StructuredLogging.kafkaPartition(partition));
              return highWatermark;
            });
    long startOffset =
        getOffset(
            () -> {
              Stopwatch watch = scope.timer(MetricNames.OFFSET_FOR_TIMES_LATENCY).start();
              return offsetOf(
                  adminClient.offsetsForTimes(ImmutableMap.of(topicPartition, startTimestamp)),
                  topicPartition,
                  consumerGroup,
                  -1,
                  watch);
            },
            // offsetForTimes may return null result of querying timestamp >
            // highwatermark timestamp, fallback endOffset in this case.
            () -> {
              logger.warn(
                  "failed to get start offset, falling back to end offset",
                  StructuredLogging.kafkaTopic(topic),
                  StructuredLogging.kafkaGroup(consumerGroup),
                  StructuredLogging.kafkaPartition(partition));
              return endOffset;
            });

    assertValidOffsets(startOffset, endOffset);
    return new OffsetRange(startOffset, endOffset);
  }

  @VisibleForTesting
  static void assertValidTimestamps(long startMs, long endMs) {
    // users might want to keep the start timestamp to be 0.
    Preconditions.checkArgument(startMs >= 0, "start timestamp %s must be >= 0", startMs);
    Preconditions.checkArgument(endMs >= 0, "end timestamp %s must be >= 0", endMs);
    long currentTime = System.currentTimeMillis();
    Preconditions.checkArgument(
        endMs < currentTime, "end timestamp %s must be < current timestamp %s", endMs, currentTime);
    Preconditions.checkArgument(
        startMs <= endMs, "start timestamp %s must be <= end timestamp %s", startMs, endMs);
  }

  @VisibleForTesting
  static void assertValidOffsets(long startOffset, long endOffset) {
    Preconditions.checkArgument(
        startOffset <= endOffset,
        "start offset %s must be <= end offset %s",
        startOffset,
        endOffset);
  }

  @VisibleForTesting
  static long getOffset(Supplier<Long> offsetSupplier, Supplier<Long> defaultSupplier) {
    Long offset = offsetSupplier.get();
    if (offset == null || offset < 0) {
      return defaultSupplier.get();
    }
    return offset;
  }

  private static long offsetOf(
      ListOffsetsResult listOffsetsResult,
      TopicPartition topicPartition,
      String consumerGroup,
      long defaultValue,
      Stopwatch stopwatch) {
    if (listOffsetsResult == null) {
      return defaultValue;
    }

    try {
      return listOffsetsResult
          .partitionResult(topicPartition)
          .get(KAFKA_ADMIN_CLIENT_LIST_OFFSETS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          .offset();
    } catch (Exception e) {
      logger.warn(
          "failed to get offset for future result",
          StructuredLogging.kafkaTopic(topicPartition.topic()),
          StructuredLogging.kafkaGroup(consumerGroup),
          StructuredLogging.kafkaPartition(topicPartition.partition()),
          e);
      return defaultValue;
    }
  }

  private static class MetricNames {
    private static final String OFFSET_FOR_TIMES_LATENCY =
        "creator.job.create.offsetForTimes.latency";
    private static final String BEGINNING_OFFSETS_LATENCY =
        "creator.job.create.beginningOffsets.latency";
    private static final String END_OFFSETS_LATENCY = "creator.job.create.endOffsets.latency";
  }
}
