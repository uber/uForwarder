package com.uber.data.kafka.datatransfer.controller.storage;

import org.springframework.context.SmartLifecycle;

/**
 * IdProvider dispenses ids for each item.
 *
 * <p>An id is a cluster globally unique identifier for the item.
 *
 * @param <K> type of the id.
 * @param <V> type of the item.
 */
public interface IdProvider<K, V> extends SmartLifecycle {
  /**
   * Returns the key for this item.
   *
   * @implSpec: a id is not necessarily deterministic for each item. e.g., for worker, id could be a
   *     unique sequence id that is generated from ZK, which increments between calls to getId.
   *     e.g., for job, a id could be to topic name, which is deterministically determines from the
   *     job.
   */
  K getId(V item) throws Exception;
}
