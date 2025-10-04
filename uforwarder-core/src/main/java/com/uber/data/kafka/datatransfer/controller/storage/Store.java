package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.api.core.InternalApi;
import com.google.protobuf.Message;
import com.uber.data.kafka.datatransfer.common.ReadStore;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.springframework.context.SmartLifecycle;

/**
 * Store is an interface for accessing data
 *
 * @implSpec there is no assumption of atomicity at the interface level.
 * @implSpec there is no assumption of strong consistency at the interface level.
 * @implSpec All methods on this class must be threadsafe.
 *     <p>TODO (T4575491): expose async API that returns CompletableFuture to increase concurrency
 *     of zk operations.
 */
@InternalApi
public interface Store<K, V extends Message> extends SmartLifecycle, ReadStore<K, V> {
  /**
   * Initialized loads any state/cache necessary for a particular store implementation.
   *
   * <p>Callers must call initialized before calling start.
   *
   * @return a CompletableFuture with a list of all data that is in the store on load. Callers may
   *     opt to use this to load state on start.
   */
  CompletableFuture<Map<K, Versioned<V>>> initialized() throws Exception;

  /**
   * create a new item in the store.
   *
   * <p>The user cannot specify the key for insertion. Rather, the key is provided by the
   * implementation.
   *
   * @param item to insert.
   * @param keyAssigner is a function that is called with the key that was assigned to this item and
   *     the item. The returned item will be the item that is actually inserted into the store. This
   *     callback allows the user to embed the key into the item.
   *     <p>TODO (T4575629): move keyAssigner parameter to constructor since it is based on the
   *     types and not specific to an object.
   */
  Versioned<V> create(V item, BiFunction<K, V, V> keyAssigner) throws Exception;

  /**
   * Blocking put of an item into the store.
   *
   * <p>The behavior of put varies based on the implementation:
   *
   * <ol>
   *   <li>If BufferedWriteDecorator is enabled, writes are stored in an in-memory buffer and
   *       periodically snapshot to persistent storage. In this case, the blocking call returns
   *       successfully once persisted to in-memory buffer.
   *   <li>In default mode, it performs a blocking write that returns on successful write to
   *       persistent storage.
   * </ol>
   *
   * @param id for this item. This is the "primary key".
   * @param item to insert.
   */
  void put(K id, Versioned<V> item) throws Exception;

  /**
   * Async put of an item into the store.
   *
   * <p>The behavior of put varies based on the implementation:
   *
   * <ol>
   *   <li>If BufferedWriteDecorator is enabled, writes are stored in an in-memory buffer and
   *       periodically snapshot to persistent storage. In this case, the completion stage completes
   *       successfully once persisted to in-memory buffer.
   *   <li>In default mode, it performs a async write that completes successfully persistent
   *       storage.
   * </ol>
   *
   * @param id for this item. This is the "primary key".
   * @param item to insert.
   * @return an CompletionStage which returns the result when the put operation finishes.
   */
  CompletionStage<Void> putAsync(K id, Versioned<V> item);

  /**
   * Put through an item into persistent storage, bypassing any in-memory storage.
   *
   * @param id for this item. This is the "primary key".
   * @param item to insert.
   */
  void putThrough(K id, Versioned<V> item) throws Exception;

  /**
   * Remove an entry from the store.
   *
   * @param id for this item to remove.
   */
  void remove(K id) throws Exception;
}
