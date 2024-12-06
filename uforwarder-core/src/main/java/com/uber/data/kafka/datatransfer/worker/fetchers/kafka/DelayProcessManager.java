package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * DelayProcessManager is an interface to manage the delayed topic partitions and their unprocessed
 * records.
 */
public interface DelayProcessManager<K, V> extends Closeable {

  /* NOOP implements a default DelayProcessManager which does nothing */
  DelayProcessManager NOOP =
      new DelayProcessManager() {

        // Default implementation for checking whether a topic partition should be delayed.
        @Override
        public boolean shouldDelayProcess(ConsumerRecord record) {
          return false;
        }

        // Default implementation to pause a topic partition. This operation is not supported.
        // It should never reach here as shouldDelayProcess function always returns false.
        @Override
        public void pausedPartitionsAndRecords(TopicPartition tp, List unprocessedRecords) {
          throw new UnsupportedOperationException();
        }

        // Default implementation for resuming paused topic partitions.
        // It returns an empty list as shouldDelayProcess function always returns false.
        @Override
        public Map<TopicPartition, List<ConsumerRecord>> resumePausedPartitionsAndRecords() {
          return Collections.emptyMap();
        }

        // Default implementation for deleting paused topic partitions.
        @Override
        public void delete(Collection tps) {}

        // Default implementation for getting paused topic partitions.
        @Override
        public List<TopicPartition> getAll() {
          return Collections.emptyList();
        }

        // Default implementation for deleting paused topic partitions.
        @Override
        public void close() {}
      };

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
  void pausedPartitionsAndRecords(TopicPartition tp, List<ConsumerRecord<K, V>> unprocessedRecords);

  /**
   * Gets the list of topic partitions and their records that could be resumed.
   *
   * @return a map of topic partitions and their records that could be resumed
   */
  Map<TopicPartition, List<ConsumerRecord<K, V>>> resumePausedPartitionsAndRecords();

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
