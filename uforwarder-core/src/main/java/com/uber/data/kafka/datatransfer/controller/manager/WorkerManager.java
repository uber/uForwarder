package com.uber.data.kafka.datatransfer.controller.manager;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.WorkerState;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.m3.tally.Scope;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

public final class WorkerManager {
  public static final Logger logger = LoggerFactory.getLogger(WorkerManager.class);
  private final Scope scope;
  private final Store<Long, StoredWorker> workerStore;
  private final LeaderSelector leaderSelector;

  WorkerManager(Scope scope, Store<Long, StoredWorker> workerStore, LeaderSelector leaderSelector)
      throws Exception {
    this.scope = scope;
    this.workerStore = workerStore;
    this.leaderSelector = leaderSelector;

    // Notice that worker store initialization is necessary, do not remove the code.
    // More details:
    // The worker store is decorated by TTLDecorator, and all workers' TTL is initialized by TTL
    // decorator's initialized method. If workers' TTL is not initialized, dead workers cannot be
    // removed.
    workerStore
        .initialized()
        .whenComplete(
            (r, t) -> {
              if (t != null) {
                logger.error("failed to initialize worker store", t);
              } else {
                logger.debug("successfully initialized worker store");
              }
            });
  }

  @Scheduled(fixedDelayString = "${master.manager.worker.metricsInterval}")
  public void logAndMetrics() {
    if (!leaderSelector.isLeader()) {
      logger.debug("skipped logAndMetrics because of current instance is not leader");
      return;
    }
    boolean isFailure = false;
    try {
      Map<Long, Versioned<StoredWorker>> workers = workerStore.getAll();
      Map<WorkerState, Integer> stateCountMap = new HashMap<>();
      for (Versioned<StoredWorker> worker : workers.values()) {
        stateCountMap.merge(
            worker.model().getState(), 1, (oldValue, newValue) -> oldValue + newValue);
      }
      for (Map.Entry<WorkerState, Integer> entry : stateCountMap.entrySet()) {
        scope
            .tagged(ImmutableMap.of("worker_state", entry.getKey().name()))
            .gauge("manager.worker.state.count")
            .update((double) entry.getValue());
        logger.info(
            entry.getKey().toString() + " count",
            StructuredLogging.count(entry.getValue()),
            StructuredLogging.workerState(entry.getKey()));
      }
    } catch (Throwable t) {
      isFailure = true;
      logger.error("worker manager log and metrics heartbeat failed", t);
      scope.counter("manager.worker.heartbeat.failed").inc(1);
    } finally {
      if (!isFailure) {
        logger.debug("worker manager log and metrics heartbeat success");
        scope.counter("manager.worker.heartbeat.success").inc(1);
      }
    }
  }
}
