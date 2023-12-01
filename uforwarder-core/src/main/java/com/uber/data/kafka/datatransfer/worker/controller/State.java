package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.api.core.InternalApi;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import com.uber.m3.tally.Scope;
import com.uber.m3.util.Duration;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;

/**
 * The Controller State Machine has 3 states: 1. Connecting 2. Registering 3. Working
 *
 * <p>The state transitions are as follows:
 *
 * <pre>
 *                  Failed to
 *                  open GRPC
 *                  connection
 *                  +-----+
 *                  |     |
 *                  v     |
 *               +--+-----+----+
 *               | Connecting  |
 *               +--+-----+----+
 *                  |     ^
 *       Open GRPC  |     | Failed to
 *       Connection |     | Register Controller
 *       to Master  |     | with Master
 *                  v     |
 *               +--+-----+---+
 *               |Registering |
 *               +--+-----+---+
 *                  |     ^
 *                  |     |
 * Successfully     |     |  Heartbeat failed
 * Register Controller  |     |  and lease expired
 * with Master      |     |
 *                  |     |
 *                  v     |
 *               +--+-----+---+
 *        +------+  Working   +-----+
 *        |      +--+-----+---+     |
 *        |         ^     ^         |
 *        |         |     |         |
 *        +---------+     +---------+
 *  Successfully             Heartbeat failed
 *  Heartbeat                but lease not expired
 *  with Master
 *  and mark success
 *  with Lease
 *
 *  </pre>
 */
@InternalApi
abstract class State {
  protected final Logger logger;
  protected final CoreInfra infra;

  // Node identification information
  protected final Node worker;
  protected final ControllerClient.Factory controllerClientFactory;
  protected final Lease lease;
  protected final java.time.Duration heartbeatTimeout;

  // Callbacks to user pluggable code
  protected final Controllable controllable;

  protected final ExecutorService commandExecutorService;

  State(
      Logger logger,
      CoreInfra infra,
      Node worker,
      ControllerClient.Factory controllerClientFactory,
      Lease lease,
      java.time.Duration heartbeatTimeout,
      ExecutorService commandExecutorService,
      Controllable controllable) {
    this.logger = logger;
    this.infra = infra;
    this.worker = worker;
    this.controllerClientFactory = controllerClientFactory;
    this.lease = lease;
    this.heartbeatTimeout = heartbeatTimeout;
    this.commandExecutorService = commandExecutorService;
    this.controllable = controllable;
  }

  public abstract State nextState();

  protected void markSuccess(String msg, String from, String to, Duration latency) {
    Scope taggedScope =
        infra
            .scope()
            .tagged(
                ImmutableMap.of(
                    MetricsTags.FROM_STATE, from,
                    MetricsTags.TO_STATE, to));
    taggedScope.timer(MetricsNames.STATE_TRANSITION_LATENCY).record(latency);
    taggedScope.counter(MetricsNames.STATE_TRANSITION_SUCCESS).inc(1);
    logger.info(String.format("[%s -> %s] " + msg, from, to));
  }

  protected void markSuccess(
      String msg, String from, String to, Duration latency, String master, boolean debugLog) {
    infra
        .scope()
        .tagged(
            ImmutableMap.of(
                MetricsTags.FROM_STATE, from,
                MetricsTags.TO_STATE, to))
        .counter(MetricsNames.STATE_TRANSITION_SUCCESS)
        .inc(1);
    // debug level used for StateWorking to avoid spamming log file with heartbeat log
    if (debugLog) {
      logger.debug(
          String.format("[%s -> %s] " + msg, from, to), StructuredLogging.masterHostPort(master));
    } else {
      logger.info(
          String.format("[%s -> %s] " + msg, from, to), StructuredLogging.masterHostPort(master));
    }
  }

  protected void markError(String msg, String from, String to, Duration latency, Throwable e) {
    infra
        .scope()
        .tagged(
            ImmutableMap.of(
                MetricsTags.FROM_STATE, from,
                MetricsTags.TO_STATE, to))
        .counter(MetricsNames.STATE_TRANSITION_ERROR)
        .inc(1);
    logger.error(String.format("[%s -> %s] " + msg, from, to), e);
  }

  protected void markError(
      String msg, String from, String to, Duration latency, String master, Throwable e) {
    infra
        .scope()
        .tagged(
            ImmutableMap.of(
                MetricsTags.FROM_STATE, from,
                MetricsTags.TO_STATE, to))
        .counter(MetricsNames.STATE_TRANSITION_ERROR)
        .inc(1);
    logger.error(
        String.format("[%s -> %s] " + msg, from, to), StructuredLogging.masterHostPort(master), e);
  }

  protected void markWarn(
      String msg, String from, String to, Duration latency, String master, Throwable e) {
    infra
        .scope()
        .tagged(
            ImmutableMap.of(
                MetricsTags.FROM_STATE, from,
                MetricsTags.TO_STATE, to))
        .counter(MetricsNames.STATE_TRANSITION_ERROR_RETRY)
        .inc(1);
    logger.warn(
        String.format("[%s -> %s] " + msg, from, to), StructuredLogging.masterHostPort(master), e);
  }

  protected void assertValidWorker(long workerId) throws Exception {
    if (workerId == WorkerUtils.UNSET_WORKER_ID) {
      throw new Exception("invalid worker_id 0");
    }
  }

  static class MetricsNames {
    static final String STATE_TRANSITION_LATENCY = "controller.state.transition.latency";
    static final String STATE_TRANSITION_SUCCESS = "controller.state.transition.success";
    static final String STATE_TRANSITION_ERROR = "controller.state.transition.error";
    static final String STATE_TRANSITION_ERROR_RETRY = "controller.state.transition.error.retry";

    static final String HEARTBEAT_LATENCY = "controller.heartbeat.latency";
    static final String HEARTBEAT_SUCCESS = "controller.heartbeat.success";
    static final String HEARTBEAT_FAILURE = "controller.heartbeat.failure";
    static final String HEARTBEAT_REDIRECT = "controller.heartbeat.redirect";

    static final String REGISTER_WORKER_LATENCY = "controller.register.worker.latency";
    static final String REGISTER_WORKER_SUCCESS = "controller.register.worker.success";
    static final String REGISTER_WORKER_FAILURE = "controller.register.worker.failure";
    static final String REGISTER_WORKER_REDIRECT = "controller.register.worker.redirect";
  }

  static class MetricsTags {
    static final String STATE = "state";
    static final String FROM_STATE = "from_state";
    static final String TO_STATE = "to_state";
  }
}
