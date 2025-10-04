package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.HostResolver;
import com.uber.data.kafka.datatransfer.common.ManagedChannelFactory;
import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

@InternalApi
final class GrpcController implements Runnable, SmartLifecycle {
  private static final Logger logger = LoggerFactory.getLogger(GrpcController.class);

  private volatile State state;

  private final ScheduledExecutorService heartbeatExecutor;
  private final ExecutorService commandExecutors;
  private final GrpcControllerConfiguration config;

  private final AtomicBoolean running;

  GrpcController(
      GrpcControllerConfiguration config,
      CoreInfra infra,
      Node worker,
      HostResolver masterResolver,
      Controllable controllable,
      ManagedChannelFactory channelFactory) {
    this.config = config;
    this.running = new AtomicBoolean(false);
    this.heartbeatExecutor =
        infra.contextManager().wrap(Executors.newSingleThreadScheduledExecutor());
    // it is safe to have multi-threaded command executor the command architecture should rely on
    // eventually consistent convergence of expected state and ideal state. There should be
    // no assumptions of strictly serializable order for command and job updates.
    commandExecutors =
        infra
            .contextManager()
            .wrap(Executors.newFixedThreadPool(config.getCommandExecutorPoolSize()));
    state =
        new StateConnecting(
            infra,
            worker,
            new ControllerClient.Factory(masterResolver, channelFactory::newManagedChannel, infra),
            new Lease(config.getWorkerLease().toMillis()),
            config.getHeartbeatTimeout(),
            commandExecutors,
            controllable);
  }

  /** Start the perioidc peartbeat thread for this worker. */
  @Override
  public void start() {
    // Previously, we use scheduleAtFixedRate. We change to scheduleWithFixedDelay because if a
    // previous heartbeat gets blocked, it does not make sense to schedule more heartbeats, it only
    // overwhelms the master.
    heartbeatExecutor.scheduleWithFixedDelay(
        this, 0, config.getHeartbeatInterval().toMillis(), TimeUnit.MILLISECONDS);
    running.set(true);
    logger.info("GRPC controller started");
  }

  /** Stop the heartbeat thread for this worker. */
  @Override
  public void stop() {
    heartbeatExecutor.shutdown();
    commandExecutors.shutdown();
    running.set(false);
    logger.info("GRPC controller stopped");
  }

  /** Returns true if the gRPC controller heartbeat thread is running. */
  @Override
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Run executes one tick of the heartbeat thread by incrementing the worker state machine to the
   * next state.
   *
   * @implSpec run should not be invoked directly. Use start/stop lifecycle methods to start the
   *     heartbeat thread instead.
   */
  @Override
  public void run() {
    state = state.nextState();
  }

  /**
   * @return a read-only copy of state that is useful for testing.
   */
  @VisibleForTesting
  State getState() {
    return state;
  }
}
