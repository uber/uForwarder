package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import com.uber.m3.util.Duration;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InternalApi
class StateConnecting extends State {
  static final String STATE = StateConnecting.class.getSimpleName();
  private static final Logger loggerPrototype = LoggerFactory.getLogger(StateConnecting.STATE);

  @VisibleForTesting
  StateConnecting(
      CoreInfra infra,
      Node worker,
      ControllerClient.Factory controllerClientFactory,
      Lease lease,
      java.time.Duration heartbeatTimeout,
      ExecutorService commandExecutorService,
      Controllable controllable) {
    super(
        loggerPrototype,
        infra.tagged(ImmutableMap.of(MetricsTags.STATE, STATE)),
        worker,
        controllerClientFactory,
        lease,
        heartbeatTimeout,
        commandExecutorService,
        controllable);
  }

  static StateConnecting from(State state) {
    return new StateConnecting(
        state.infra,
        state.worker,
        state.controllerClientFactory,
        state.lease,
        state.heartbeatTimeout,
        state.commandExecutorService,
        state.controllable);
  }

  @Override
  public String toString() {
    return STATE;
  }

  @Override
  public State nextState() {
    boolean isFailure = false;
    long startNs = System.nanoTime();
    try {
      ControllerClient controllerClient = controllerClientFactory.connect();
      return StateRegistering.from(this, controllerClient);
    } catch (Exception e) {
      isFailure = true;
      markError(
          "failed to connect to master",
          StateConnecting.STATE,
          StateConnecting.STATE,
          Duration.between(startNs, System.nanoTime()),
          e);
      return this;
    } finally {
      if (!isFailure) {
        markSuccess(
            "successfully connect to master",
            StateConnecting.STATE,
            StateRegistering.STATE,
            Duration.between(startNs, System.nanoTime()));
      }
    }
  }
}
