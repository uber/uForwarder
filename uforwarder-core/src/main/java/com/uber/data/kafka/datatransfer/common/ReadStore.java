package com.uber.data.kafka.datatransfer.common;

import com.google.protobuf.Message;
import java.util.Map;
import java.util.function.Function;
import org.apache.curator.x.async.modeled.versioned.Versioned;

/**
 * ReadStore exposes read-only methods for accessing information.
 *
 * <p>We expose a ReadStore so that we can provide read-only convenience shims on top of full
 * stores. e.g., JobStore is a ReadStore on top of JobGroupStore.
 */
public interface ReadStore<K, V extends Message> {
  /**
   * GetAll returns all elements that currently stored.
   *
   * @return list of elements found in the store.
   */
  Map<K, Versioned<V>> getAll() throws Exception;

  /**
   * GetAll returns all elements that are currently stored that pass the {@code selector} check.
   *
   * @param selector is a filter that is used to check whether an element should be returned. Only
   *     elements that return true are returned in the output list.
   * @return list of elements that pass the {@code selector} check.
   */
  Map<K, Versioned<V>> getAll(Function<V, Boolean> selector) throws Exception;

  /**
   * Get performs a point lookup of the Store based on the {@code id}.
   *
   * @param id to query.
   * @return element that was found in the store.
   * @throws Exception if the element cannot be found.
   */
  Versioned<V> get(K id) throws Exception;

  /**
   * GetThrough performs a point lookup of the Store based on the {@code id} bypassing the cache.
   *
   * @param id to query.
   * @return element that was found in the store.
   * @throws Exception if the element cannot be found.
   */
  Versioned<V> getThrough(K id) throws Exception;
}
