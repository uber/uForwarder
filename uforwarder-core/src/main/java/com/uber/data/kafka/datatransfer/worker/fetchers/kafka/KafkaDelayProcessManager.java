package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.m3.tally.Scope;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaDelayProcessManager is a class to manage paused/delayed topic partitions and their
 * unprocessed records.
 */
public class KafkaDelayProcessManager<K, V> implements DelayProcessManager<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDelayProcessManager.class);
  private static final String REASON = "delayed-topic-partition";
  private final Scope scope;
  private final String consumerGroup;
  private final int processingDelayMs;
  private final Consumer<K, V> kafkaConsumer;

  /**
   * A map to store the paused topic partitions and their unprocessed records. When the value of a
   * topic partition is an empty list in the map, it means the topic partition is not yet resumed in
   * KafkaConsumer but its records is being processing in KCP.
   */
  private final ConcurrentMap<TopicPartition, List<ConsumerRecord<K, V>>> delayedRecords =
      new ConcurrentHashMap();

  public KafkaDelayProcessManager(
      Scope scope, String consumerGroup, int processingDelayMs, Consumer<K, V> kafkaConsumer) {
    this.scope = scope;
    this.consumerGroup = consumerGroup;
    this.processingDelayMs = processingDelayMs;
    this.kafkaConsumer = kafkaConsumer;
  }

  // Determines whether to delay process a record based on the delay time.
  @Override
  public boolean shouldDelayProcess(ConsumerRecord<K, V> record) {
    if (processingDelayMs > 0) {
      long deadline = record.timestamp() + processingDelayMs;
      return deadline > System.currentTimeMillis();
    }
    return false;
  }

  // Pauses the topic partition and stores its unprocessed records.
  @Override
  public void pausedPartitionsAndRecords(
      TopicPartition tp, List<ConsumerRecord<K, V>> unprocessedRecords) {
    Preconditions.checkArgument(
        unprocessedRecords.size() > 0,
        "It requires unprocessedRecords to be not empty for topic partition %s",
        tp);
    Preconditions.checkArgument(
        !delayedRecords.containsKey(tp), "The topic partition %s is already in delayedRecords", tp);

    kafkaConsumer.pause(Collections.singleton(tp));
    delayedRecords.put(tp, unprocessedRecords);

    LOGGER.info(
        "kafka.pause",
        StructuredLogging.kafkaGroup(consumerGroup),
        StructuredLogging.kafkaTopic(tp.topic()),
        StructuredLogging.kafkaPartition(tp.partition()),
        StructuredLogging.reason(REASON));
    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaGroup(consumerGroup)
                .setKafkaTopic(tp.topic())
                .setKafkaPartition(tp.partition())
                .build())
        .gauge(MetricNames.TOPIC_PARTITION_PAUSED)
        .update(1);
  }

  @VisibleForTesting
  List<ConsumerRecord<K, V>> getRecords(TopicPartition tp) {
    if (delayedRecords.containsKey(tp)) {
      return delayedRecords.get(tp);
    }
    return Collections.emptyList();
  }

  // Gets the list of topic partitions that could be resumed to process.
  @Override
  public Map<TopicPartition, List<ConsumerRecord<K, V>>> resumePausedPartitionsAndRecords() {
    ImmutableList.Builder<TopicPartition> resumedPartitions = ImmutableList.builder();
    ImmutableMap.Builder<TopicPartition, List<ConsumerRecord<K, V>>> resumedRecords =
        ImmutableMap.builder();

    for (Iterator<Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>>> it =
            delayedRecords.entrySet().iterator();
        it.hasNext(); ) {
      Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry = it.next();
      if (entry.getValue().isEmpty()) {
        // The topic partition is not yet resumed in KafkaConsumer but its records have been
        // processed.
        resumedPartitions.add(entry.getKey());
        it.remove();
      } else if (!shouldDelayProcess(entry.getValue().get(0))) {
        // resume processing the remaining records for the topic partition, but not resume fetching
        // new records.
        resumedRecords.put(entry.getKey(), entry.getValue());
        delayedRecords.put(entry.getKey(), Collections.emptyList());
      }
    }

    kafkaConsumer.resume(resumedPartitions.build());
    for (TopicPartition tp : resumedPartitions.build()) {
      LOGGER.info(
          "kafka.resume",
          StructuredLogging.kafkaGroup(consumerGroup),
          StructuredLogging.kafkaTopic(tp.topic()),
          StructuredLogging.kafkaPartition(tp.partition()),
          StructuredLogging.reason(REASON));
      scope
          .tagged(
              StructuredTags.builder()
                  .setKafkaGroup(consumerGroup)
                  .setKafkaTopic(tp.topic())
                  .setKafkaPartition(tp.partition())
                  .build())
          .gauge(MetricNames.TOPIC_PARTITION_RESUME)
          .update(1);
    }

    return resumedRecords.build();
  }

  @Override
  public void delete(Collection<TopicPartition> tps) {
    for (TopicPartition tp : tps) {
      if (delayedRecords.containsKey(tp)) {
        delayedRecords.remove(tp);
      }
    }
  }

  @Override
  public List<TopicPartition> getAll() {
    return ImmutableList.copyOf(delayedRecords.keySet());
  }

  @Override
  public void close() {
    delayedRecords.clear();
  }

  private static class MetricNames {
    static final String TOPIC_PARTITION_PAUSED = "fetcher.kafka.topic.partition.paused";
    static final String TOPIC_PARTITION_RESUME = "fetcher.kafka.topic.partition.resume";
  }
}
