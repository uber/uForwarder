package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.instrumentation.Instrumentation;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TTLDecorator adds functionality to TTL data that has not been written for some configurable
 * amount of time.
 */
public final class TTLDecorator<K, V extends Message> implements Store<K, V>, LeaderLatchListener {

  private static final Logger logger = LoggerFactory.getLogger(TTLDecorator.class);

  private final CoreInfra infra;
  private final Store<K, V> impl;
  private final ScheduledExecutorService ttlExpirationExecutor;
  private final ConcurrentMap<K, ScheduledFuture> ttlExpirationFutures;
  private final Duration ttl;
  private final Function<V, K> getIdFn;
  private final LeaderSelector leaderSelector;

  @VisibleForTesting
  TTLDecorator(
      CoreInfra infra,
      Store<K, V> impl,
      Function<V, K> getIdFn,
      ScheduledExecutorService ttlExpirationExecutor,
      ConcurrentMap<K, ScheduledFuture> ttlExpirationFutures,
      Duration ttl,
      LeaderSelector leaderSelector) {
    Preconditions.checkArgument(ttl.toMillis() > 0);
    this.infra = infra;
    this.impl = impl;
    this.ttlExpirationExecutor = ttlExpirationExecutor;
    this.ttlExpirationFutures = ttlExpirationFutures;
    this.ttl = ttl;
    this.getIdFn = getIdFn;
    this.leaderSelector = leaderSelector;
  }

  static <K, V extends Message> TTLDecorator decorate(
      CoreInfra infra,
      Store<K, V> impl,
      Function<V, K> getIdFn,
      Duration ttl,
      LeaderSelector leaderSelector) {
    return new TTLDecorator<>(
        infra,
        impl,
        getIdFn,
        infra.contextManager().wrap(Executors.newSingleThreadScheduledExecutor()),
        new ConcurrentHashMap<>(),
        ttl,
        leaderSelector);
  }

  @VisibleForTesting
  Versioned<V> updateTTL(Versioned<V> item) {
    return Instrumentation.instrument.withRuntimeException(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          K id = getIdFn.apply(item.model());
          // Invalidate old ttl expiration future
          ttlExpirationFutures.merge(
              id,
              ttlExpirationExecutor.schedule(
                  new ExpirationTask<>(
                      infra,
                      this,
                      task ->
                          ttlExpirationExecutor.schedule(
                              task, ttl.toMillis(), TimeUnit.MILLISECONDS),
                      id,
                      leaderSelector,
                      0),
                  ttl.toMillis(),
                  TimeUnit.MILLISECONDS),
              (oldValue, newValue) -> {
                // merge function will guarantee oldValue is never null
                oldValue.cancel(false);
                logger.debug(
                    "canceling old ttl expiration task", StructuredLogging.id(id.toString()));
                infra.scope().counter("ttl.expiration.cancel").inc(1);
                return newValue;
              });
          return item;
        },
        "ttl.update");
  }

  @Override
  public void start() {
    impl.start();
    leaderSelector.registerListener(this);
  }

  @Override
  public void stop() {
    ttlExpirationExecutor.shutdown();
    impl.stop();
  }

  @Override
  public boolean isRunning() {
    return impl.isRunning();
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> initialized() throws Exception {
    CompletableFuture<Map<K, Versioned<V>>> future =
        impl.initialized()
            .thenApply(
                items -> {
                  if (!leaderSelector.isLeader()) {
                    logger.debug("skipped updateTTL because of instance is not leader");
                    return items;
                  }
                  for (Map.Entry<K, Versioned<V>> item : items.entrySet()) {
                    updateTTL(item.getValue());
                  }
                  logger.info("set initial item TTLs", StructuredLogging.count(items.size()));
                  return items;
                });
    return future;
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
  public Versioned<V> get(K id) throws Exception {
    return updateTTL(impl.get(id));
  }

  @Override
  public Versioned<V> getThrough(K id) throws Exception {
    return updateTTL(impl.getThrough(id));
  }

  @Override
  public Versioned<V> create(V id, BiFunction<K, V, V> creator) throws Exception {
    return updateTTL(impl.create(id, creator));
  }

  @Override
  public void put(K id, Versioned<V> item) throws Exception {
    impl.put(id, item);
    updateTTL(item);
  }

  @Override
  public CompletionStage<Void> putAsync(K id, Versioned<V> item) {
    return impl.putAsync(id, item)
        .thenApply(
            r -> {
              updateTTL(item);
              return r;
            });
  }

  @Override
  public void putThrough(K id, Versioned<V> item) throws Exception {
    impl.putThrough(id, item);
    updateTTL(item);
  }

  @Override
  public void remove(K id) throws Exception {
    impl.remove(id);
  }

  // TODO (T4678197): add metrics for age of data into store
  @Override
  public void isLeader() {
    Instrumentation.instrument.returnVoidCatchThrowable(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          Map<K, Versioned<V>> items = impl.getAll();
          for (Map.Entry<K, Versioned<V>> item : items.entrySet()) {
            updateTTL(item.getValue());
          }
        },
        "ttl.leader.acquired");
  }

  @Override
  public void notLeader() {}

  @VisibleForTesting
  static final class ExpirationTask<K, V extends Message> implements Runnable {
    private final Consumer<ExpirationTask> reschedule;
    private final CoreInfra infra;
    private final Store<K, V> store;
    private final K id;
    private final long attempt;
    private LeaderSelector leaderSelector;

    ExpirationTask(
        CoreInfra infra,
        Store<K, V> store,
        Consumer<ExpirationTask> reschedule,
        K id,
        LeaderSelector leaderSelector,
        long attempt) {
      this.infra = infra;
      this.store = store;
      this.reschedule = reschedule;
      this.id = id;
      this.leaderSelector = leaderSelector;
      this.attempt = attempt;
    }

    @Override
    public void run() {
      leaderSelector.runIfLeader(
          "ttl.expiration",
          () -> {
            Instrumentation.instrument.returnVoidCatchThrowable(
                logger,
                infra.scope(),
                infra.tracer(),
                () -> {
                  try {
                    store.remove(id);
                    logger.info("removed TTL expired item", StructuredLogging.id(id.toString()));
                  } catch (Throwable t) {
                    // Upon failure, we reschedule the expiration task again.
                    // ExpirationTask occurs when worker fails to heartbeat in time and therefore
                    // its lease is expired. We must not lose the expiration task.
                    // If we lose an ExpirationTask, the master will believe that the jobs is
                    // assigned valid
                    // workers
                    // but the worker is not actually working on it.
                    // If the attempt count is too high, someone should be paged to look into the
                    // issue.
                    reschedule.accept(
                        new ExpirationTask<>(
                            infra, store, reschedule, id, leaderSelector, attempt + 1));
                    throw t;
                  }
                },
                "ttl.expiration");
          });
    }

    public long getAttempt() {
      return attempt;
    }
  }
}
