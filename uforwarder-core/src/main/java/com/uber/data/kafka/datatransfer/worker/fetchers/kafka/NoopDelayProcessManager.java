package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/* NoopDelayProcessManager implements a default DelayProcessManager which does nothing */
public class NoopDelayProcessManager<K, V> implements DelayProcessManager<K, V> {

  public NoopDelayProcessManager() {}

  // Default implementation for checking whether a topic partition should be delayed.
  @Override
  public boolean shouldDelayProcess(ConsumerRecord<K, V> record) {
    return false;
  }

  // Default implementation to pause a topic partition. This operation is not supported.
  // It should never reach here as shouldDelayProcess function always returns false.
  @Override
  public void pause(TopicPartition tp, List<ConsumerRecord<K, V>> records) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  // Default implementation for resuming paused topic partitions.
  // It returns an empty list as shouldDelayProcess function always returns false.
  @Override
  public Map<TopicPartition, List<ConsumerRecord<K, V>>> resume() {
    return Collections.emptyMap();
  }

  // Default implementation for deleting paused topic partitions.
  @Override
  public void delete(Collection<TopicPartition> tps) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  // Default implementation for getting paused topic partitions.
  @Override
  public List<TopicPartition> getAll() {
    return Collections.emptyList();
  }

  // Default implementation for deleting paused topic partitions.
  @Override
  public void close() {}
}
