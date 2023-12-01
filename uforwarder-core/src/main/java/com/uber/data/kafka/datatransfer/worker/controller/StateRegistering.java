package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.api.core.InternalApi;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.Participants;
import com.uber.data.kafka.datatransfer.RegisterWorkerRequest;
import com.uber.data.kafka.datatransfer.RegisterWorkerResponse;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.NodeUtils;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.util.Duration;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InternalApi
class StateRegistering extends State {

  static final String STATE = StateRegistering.class.getSimpleName();
  private static final Logger loggerPrototype = LoggerFactory.getLogger(StateRegistering.STATE);
  final ControllerClient controllerClient;

  StateRegistering(
      CoreInfra infra,
      Node worker,
      ControllerClient.Factory controllerClientFactory,
      Lease lease,
      java.time.Duration heartbeatTimeout,
      ExecutorService commandExecutorService,
      Controllable controllable,
      ControllerClient controllerClient) {
    super(
        loggerPrototype,
        infra.tagged(ImmutableMap.of(MetricsTags.STATE, STATE)),
        worker,
        controllerClientFactory,
        lease,
        heartbeatTimeout,
        commandExecutorService,
        controllable);
    this.controllerClient = controllerClient;
  }

  static StateRegistering from(State state, ControllerClient controllerClient) {
    return new StateRegistering(
        state.infra,
        state.worker,
        state.controllerClientFactory,
        state.lease,
        state.heartbeatTimeout,
        state.commandExecutorService,
        state.controllable,
        controllerClient);
  }

  @Override
  public String toString() {
    return STATE;
  }

  @Override
  public State nextState() {
    boolean isFailure = false;
    long startNs = System.nanoTime();
    Stopwatch registerWorkerTimer =
        infra.scope().timer(MetricsNames.REGISTER_WORKER_LATENCY).start();
    try {
      RegisterWorkerRequest request =
          RegisterWorkerRequest.newBuilder()
              .setParticipants(
                  Participants.newBuilder()
                      .setMaster(controllerClient.getNode())
                      .setWorker(worker)
                      .build())
              .build();
      RegisterWorkerResponse response = controllerClient.getStub().registerWorker(request);
      if (response.getParticipants().getMaster().equals(controllerClient.getNode())) {
        assertValidWorker(response.getParticipants().getWorker().getId());
        lease.success();
        return StateWorking.from(this, response.getParticipants().getWorker(), controllerClient);
      } else {
        // skipping markSuccess
        isFailure = true;
        infra
            .scope()
            .tagged(
                ImmutableMap.of(
                    StructuredLogging.FROM_MASTER_HOST,
                    controllerClient.getNode().getHost(),
                    StructuredLogging.TO_MASTER_HOST,
                    response.getParticipants().getMaster().getHost()))
            .counter(MetricsNames.REGISTER_WORKER_REDIRECT)
            .inc(1);
        ControllerClient newControllerClient = null;
        try {
          newControllerClient =
              controllerClientFactory.reconnectOnChange(
                  this.controllerClient, response.getParticipants().getMaster());
        } catch (Exception reconnectException) {
          logger.error(
              String.format(
                  "[%s -> %s] "
                      + "got redirect master response but failed to reconnect to the new master",
                  StateRegistering.STATE, StateRegistering.STATE),
              StructuredLogging.masterHostPort(
                  NodeUtils.getHostAndPortString(response.getParticipants().getMaster())),
              reconnectException);
          throw reconnectException;
        }
        markSuccess(
            "successfully redirect master",
            StateRegistering.STATE,
            StateRegistering.STATE,
            Duration.between(startNs, System.nanoTime()),
            NodeUtils.getHostAndPortString(response.getParticipants().getMaster()),
            false);
        return StateRegistering.from(this, newControllerClient);
      }
    } catch (Exception e) {
      isFailure = true;
      infra.scope().counter(MetricsNames.REGISTER_WORKER_FAILURE).inc(1);
      markError(
          "failed to register worker with master",
          StateRegistering.STATE,
          StateConnecting.STATE,
          Duration.between(startNs, System.nanoTime()),
          NodeUtils.getHostAndPortString(controllerClient.getNode()),
          e);
      try {
        controllerClient.close();
      } catch (Exception closeException) {
        // We don't take any extra action here because we have initialized the shutdown process,
        // the exception indicates that the masterClient was not successfully closed before a
        // pre-defined timeout, but the masterClient should be eventually close.
        logger.warn(
            "failed to close the master client",
            StructuredLogging.masterHostPort(
                NodeUtils.getHostAndPortString(controllerClient.getNode())),
            closeException);
      }
      return StateConnecting.from(this);
    } finally {
      registerWorkerTimer.stop();
      if (!isFailure) {
        markSuccess(
            "successfully registered worker to master",
            StateRegistering.STATE,
            StateWorking.STATE,
            Duration.between(startNs, System.nanoTime()),
            String.format(
                "%s:%d",
                controllerClient.getNode().getHost(), controllerClient.getNode().getPort()),
            false);
      }
    }
  }
}
