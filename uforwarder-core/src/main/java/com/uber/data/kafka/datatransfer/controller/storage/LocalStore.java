package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.api.core.InternalApi;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.common.ZKUtils;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.curator.x.async.modeled.versioned.Versioned;

/**
 * LocalStore is an implementation of Store interface that is backed by an in-memory map.
 *
 * <p>This is useful for testing or local development.
 */
@InternalApi
public class LocalStore<K, V extends Message> implements Store<K, V> {
  private final Map<K, Versioned<V>> storage = new ConcurrentHashMap<>();
  private final IdProvider<K, V> idProvider;

  public LocalStore(IdProvider<K, V> idProvider) {
    this.idProvider = idProvider;
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> initialized() {
    return CompletableFuture.completedFuture(getAll());
  }

  @Override
  public Map<K, Versioned<V>> getAll() {
    return Collections.unmodifiableMap(storage);
  }

  @Override
  public Map<K, Versioned<V>> getAll(Function<V, Boolean> selector) {
    ImmutableMap.Builder<K, Versioned<V>> builder = new ImmutableMap.Builder<>();
    for (Map.Entry<K, Versioned<V>> entry : storage.entrySet()) {
      if (selector.apply(entry.getValue().model())) {
        builder.put(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  @Override
  public Versioned<V> get(K id) {
    Versioned<V> item = storage.get(id);
    if (item == null) {
      throw new NoSuchElementException(String.format("%s not in local store", id));
    }
    return item;
  }

  @Override
  public Versioned<V> getThrough(K id) {
    return get(id);
  }

  @Override
  public Versioned<V> create(V item, BiFunction<K, V, V> creator) throws Exception {
    K key = idProvider.getId(item);
    Versioned<V> itemToInsert = VersionedProto.from(creator.apply(key, item), ZKUtils.NOOP_VERSION);
    final Versioned<V> previousItem = storage.putIfAbsent(key, itemToInsert);
    if (previousItem != null) {
      throw new IllegalArgumentException("item already exists");
    }
    return itemToInsert;
  }

  @Override
  public void put(K id, Versioned<V> item) {
    storage.put(id, item);
  }

  @Override
  public CompletionStage<Void> putAsync(K id, Versioned<V> item) {
    return CompletableFuture.supplyAsync(() -> storage.put(id, item)).thenApply(x -> null);
  }

  @Override
  public void putThrough(K id, Versioned<V> item) {
    storage.put(id, item);
  }

  @Override
  public void remove(K id) {
    storage.remove(id);
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public boolean isRunning() {
    return true;
  }
}
