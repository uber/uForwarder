package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * DelayProcessManager is an interface to manage the delayed topic partitions and their unprocessed
 * records.
 */
public interface DelayProcessManager<K, V> extends Closeable {

  /**
   * Determines whether to delay processing a record based on the delay time and record timestamp.
   *
   * @param record the record that to be verified whether it should be delayed
   * @return true if the record should be delayed for processing, false otherwise
   */
  boolean shouldDelayProcess(ConsumerRecord<K, V> record);

  /**
   * Pauses a topic partition and stores its unprocessed records.
   *
   * @param tp the topic partition that should be paused
   * @param unprocessedRecords the polled but unprocessed records for the topic partition
   * @return void
   */
  void pause(TopicPartition tp, List<ConsumerRecord<K, V>> unprocessedRecords);

  /**
   * Gets the list of topic partitions and their records that could be resumed.
   *
   * @return a map of topic partitions and their records that could be resumed
   */
  Map<TopicPartition, List<ConsumerRecord<K, V>>> resume();

  /**
   * Deletes the paused topic partitions and their unprocessed records.
   *
   * @param tps topic partitions that to be deleted
   * @return void
   */
  void delete(Collection<TopicPartition> tps);

  /**
   * Gets all delayed topic partitions.
   *
   * @return a list of topic partitions that are delayed
   */
  List<TopicPartition> getAll();
}
