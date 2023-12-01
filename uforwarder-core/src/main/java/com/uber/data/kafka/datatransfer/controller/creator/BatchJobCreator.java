package com.uber.data.kafka.datatransfer.controller.creator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import com.uber.data.kafka.clients.admin.Admin;
import com.uber.data.kafka.clients.admin.MultiClusterAdmin;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.m3.tally.Scope;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
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
  private static final String JOB_TYPE = "batch";
  private static final int LIST_CONSUMER_GROUP_OFFSETS_TIMEOUT_MS = 20000;

  private final MultiClusterAdmin multiClusterAdmin;
  private final Scope scope;
  private final CoreInfra infra;

  public BatchJobCreator(MultiClusterAdmin multiClusterAdmin, CoreInfra infra) {
    this.multiClusterAdmin = multiClusterAdmin;
    this.scope = infra.scope().tagged(ImmutableMap.of("mode", "batch"));
    this.infra = infra;
  }

  @Override
  public StoredJob newJob(StoredJobGroup storedJobGroup, long jobId, int partition) {
    final String cluster = storedJobGroup.getJobGroup().getKafkaConsumerTaskGroup().getCluster();
    final String topic = storedJobGroup.getJobGroup().getKafkaConsumerTaskGroup().getTopic();
    final String consumerGroup =
        storedJobGroup.getJobGroup().getKafkaConsumerTaskGroup().getConsumerGroup();
    return Instrumentation.instrument.withRuntimeException(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          TopicPartition topicPartition = new TopicPartition(topic, partition);
          long startTimestamp =
              Timestamps.toMillis(
                  storedJobGroup.getJobGroup().getKafkaConsumerTaskGroup().getStartTimestamp());
          long endTimestamp =
              Timestamps.toMillis(
                  storedJobGroup.getJobGroup().getKafkaConsumerTaskGroup().getEndTimestamp());

          assertValidTimestamps(startTimestamp, endTimestamp);
          Admin singleClusterAdminClient = multiClusterAdmin.getAdmin(cluster);

          long lowWatermark =
              singleClusterAdminClient
                  .beginningOffsets(ImmutableList.of(topicPartition))
                  .getOrDefault(topicPartition, 0L);
          long highWatermark =
              singleClusterAdminClient
                  .endOffsets(ImmutableList.of(topicPartition))
                  .getOrDefault(topicPartition, 0L);

          // there are no messages in this partition
          if (lowWatermark == highWatermark) {
            return newJob(
                scope,
                logger,
                storedJobGroup,
                JOB_TYPE,
                jobId,
                partition,
                lowWatermark,
                highWatermark);
          }

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
          Map<TopicPartition, OffsetAndTimestamp> endOffsetMap =
              singleClusterAdminClient.offsetsForTimes(
                  ImmutableMap.of(topicPartition, endTimestamp));
          Map<TopicPartition, OffsetAndTimestamp> startOffsetMap =
              singleClusterAdminClient.offsetsForTimes(
                  ImmutableMap.of(topicPartition, startTimestamp));
          KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> committedOffsetsFuture =
              singleClusterAdminClient
                  .listConsumerGroupOffsets(consumerGroup)
                  .partitionsToOffsetAndMetadata();

          long endOffset =
              getOffset(
                  endOffsetMap,
                  topicPartition,
                  () -> {
                    // offsetForTimes may return null result of querying timestamp >
                    // highwatermark timestamp, fallback endOffset in this case.
                    logger.warn(
                        "failed to get end offset, falling back to high watermark",
                        StructuredLogging.kafkaTopic(topic),
                        StructuredLogging.kafkaGroup(consumerGroup),
                        StructuredLogging.kafkaPartition(partition));
                    return singleClusterAdminClient
                        .endOffsets(ImmutableList.of(topicPartition))
                        .getOrDefault(topicPartition, 0L);
                  });
          long committedOffset =
              getCommittedOffset(
                  committedOffsetsFuture,
                  topicPartition,
                  // listConsumerGroupOffsets may error out, fallback to end offset in
                  // this case.
                  () -> {
                    logger.warn(
                        "failed to get committed offset, falling back to low watermark",
                        StructuredLogging.kafkaTopic(topic),
                        StructuredLogging.kafkaGroup(consumerGroup),
                        StructuredLogging.kafkaPartition(partition));
                    return lowWatermark;
                  });
          long startOffset =
              getOffset(
                  startOffsetMap,
                  topicPartition,
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

          return newJob(
              scope, logger, storedJobGroup, JOB_TYPE, jobId, partition, startOffset, endOffset);
        },
        "creator.job.create",
        StructuredFields.KAFKA_CLUSTER,
        cluster,
        StructuredFields.KAFKA_TOPIC,
        topic,
        StructuredFields.KAFKA_PARTITION,
        Integer.toString(partition));
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
  static long getOffset(
      Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap,
      TopicPartition topicPartition,
      Supplier<Long> defaultSupplier) {
    OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(topicPartition);
    if (offsetAndTimestamp == null || offsetAndTimestamp.offset() < 0) {
      return defaultSupplier.get();
    }
    return offsetAndTimestamp.offset();
  }

  private static long getCommittedOffset(
      KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> listConsumerGroupOffsetsFuture,
      TopicPartition topicPartition,
      Supplier<Long> defaultSupplier) {
    Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap;
    try {
      topicPartitionOffsetAndMetadataMap =
          listConsumerGroupOffsetsFuture.get(
              LIST_CONSUMER_GROUP_OFFSETS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      logger.error(
          "failed to fetch committed offset", StructuredLogging.kafkaTopic(topicPartition.topic()));
      return defaultSupplier.get();
    }
    OffsetAndMetadata offsetAndMetadata = topicPartitionOffsetAndMetadataMap.get(topicPartition);
    if (offsetAndMetadata == null || offsetAndMetadata.offset() < 0) {
      return defaultSupplier.get();
    }
    return offsetAndMetadata.offset();
  }
}
