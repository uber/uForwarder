package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.instrumentation.Instrumentation;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;

/**
 * BufferedWriteDecorator adds write through buffer into a Store implementation.
 *
 * <p>When an item is put into the store, it will first be put into an in-memory storage, which is
 * further persisted into the "permanent" storage periodically.
 */
@InternalApi
public final class BufferedWriteDecorator<K, V extends Message> implements Store<K, V>, Runnable {
  @VisibleForTesting final Map<K, Versioned<V>> writeCache;
  private final Logger logger;
  private final CoreInfra infra;
  private final Store<K, V> impl;
  private final Duration asyncWriteInterval;
  private final ScheduledExecutorService scheduledExecutorService;

  private BufferedWriteDecorator(
      Duration asyncWriteInterval, Logger logger, CoreInfra infra, Store<K, V> impl) {
    this.asyncWriteInterval = asyncWriteInterval;
    this.logger = logger;
    this.infra = infra;
    this.impl = impl;
    this.writeCache = new ConcurrentHashMap<>();
    this.scheduledExecutorService =
        infra.contextManager().wrap(Executors.newSingleThreadScheduledExecutor());
  }

  public static <K, V extends Message> Store<K, V> decorate(
      Duration writeCachePersistInterval, Logger logger, CoreInfra infra, Store<K, V> impl) {
    return new BufferedWriteDecorator<>(writeCachePersistInterval, logger, infra, impl);
  }

  /** run is scheduled to run periodically, which writes in-memory storage to permanent storage. */
  @Override
  public void run() {
    Instrumentation.instrument.returnVoidCatchThrowable(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          Map<K, Versioned<V>> itemsToWrite;
          itemsToWrite = new HashMap<>(writeCache);
          writeCache.clear();
          Set<CompletableFuture> completableFutureSet = new HashSet<>();
          itemsToWrite.forEach(
              (id, item) ->
                  completableFutureSet.add(
                      Instrumentation.instrument
                          .withExceptionalCompletion(
                              logger,
                              infra.scope(),
                              infra.tracer(),
                              () -> impl.putAsync(id, item),
                              "write.storage")
                          .toCompletableFuture()));
          CompletableFuture.allOf(completableFutureSet.toArray(new CompletableFuture[0])).get();
          scheduledExecutorService.schedule(
              this, asyncWriteInterval.toMillis(), TimeUnit.MILLISECONDS);
        },
        "write.task");
  }

  @Override
  public void start() {
    scheduledExecutorService.schedule(this, asyncWriteInterval.toMillis(), TimeUnit.MILLISECONDS);
    impl.start();
  }

  @Override
  public void stop() {
    impl.stop();
    if (!scheduledExecutorService.isShutdown()) {
      scheduledExecutorService.shutdown();
    }
  }

  @Override
  public boolean isRunning() {
    return impl.isRunning();
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> initialized() throws Exception {
    return impl.initialized();
  }

  @Override
  public Map<K, Versioned<V>> getAll() throws Exception {
    return impl.getAll();
  }

  @Override
  public Map<K, Versioned<V>> getAll(Function<V, Boolean> selector) throws Exception {
    return impl.getAll(selector);
  }

  @Override
  public Versioned<V> create(V item, BiFunction<K, V, V> keyAssigner) throws Exception {
    return impl.create(item, keyAssigner);
  }

  @Override
  public Versioned<V> get(K id) throws Exception {
    return impl.get(id);
  }

  @Override
  public Versioned<V> getThrough(K id) throws Exception {
    return impl.getThrough(id);
  }

  /**
   * Puts an item into the in-memory storage, which is further persisted into the "permanent"
   * storage periodically.
   *
   * @param id for this item. This is the "primary key".
   * @param item to insert.
   * @throws Exception if this operation fails to complete.
   */
  @Override
  public void put(K id, Versioned<V> item) throws Exception {
    writeCache.put(id, item);
  }

  @Override
  public CompletionStage<Void> putAsync(K id, Versioned<V> item) {
    writeCache.put(id, item);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void putThrough(K id, Versioned<V> item) throws Exception {
    impl.putThrough(id, item);
  }

  @Override
  public void remove(K id) throws Exception {
    writeCache.remove(id);
    impl.remove(id);
  }
}
