package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Command;
import com.uber.data.kafka.datatransfer.HeartbeatRequest;
import com.uber.data.kafka.datatransfer.HeartbeatResponse;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.Participants;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.NodeUtils;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.util.Duration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InternalApi
final class StateWorking extends State {

  static final String STATE = StateWorking.class.getSimpleName();
  // COMMAND_TIMEOUT_SCHEDULER is used to report critical metrics, have 0 corePoolSize to unblock
  // shutdown
  private static final ScheduledExecutorService COMMAND_TIMEOUT_SCHEDULER =
      Executors.newScheduledThreadPool(0);
  private static final Logger loggerPrototype = LoggerFactory.getLogger(StateWorking.STATE);
  private static final int COMMAND_TIMEOUT_MS = 2000;
  final ControllerClient controllerClient;

  @VisibleForTesting
  StateWorking(
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

  static StateWorking from(State state, Node newWorker, ControllerClient controllerClient) {
    return new StateWorking(
        state.infra,
        newWorker,
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
    final String cancelAllCommand = "cancelAll";
    final String getAllCommand = "getAll";
    final String aggregatedStage = "aggregated";
    boolean isFailure = false;
    long startNs = System.nanoTime();
    try {
      // Get Job Status from user provided JobStatusChecker.
      Collection<JobStatus> jobList = null;
      Stopwatch statusCheckTimer = infra.scope().timer(Controllable.Metrics.LATENCY).start();
      boolean jobStatusCheckerFailure = false;
      Map<String, String> getAllTags =
          ImmutableMap.of(StructuredLogging.CONTROLLABLE_COMMAND, getAllCommand);
      try {
        infra.scope().tagged(getAllTags).counter(Controllable.Metrics.CALLED).inc(1);
        logger.debug(
            Controllable.Metrics.CALLED,
            StructuredLogging.workerStage(aggregatedStage),
            StructuredLogging.controllableCommand(getAllCommand));
        jobList = controllable.getJobStatus();
      } catch (Exception e) {
        jobStatusCheckerFailure = true;
        infra.scope().tagged(getAllTags).counter(Controllable.Metrics.FAILURE).inc(1);
        logger.warn(
            Controllable.Metrics.FAILURE,
            StructuredLogging.workerStage(aggregatedStage),
            StructuredLogging.controllableCommand(getAllCommand),
            e);
      } finally {
        statusCheckTimer.stop();
        if (!jobStatusCheckerFailure) {
          infra.scope().tagged(getAllTags).counter(Controllable.Metrics.SUCCESS).inc(1);
          logger.debug(
              Controllable.Metrics.SUCCESS,
              StructuredLogging.workerStage(aggregatedStage),
              StructuredLogging.controllableCommand(getAllCommand));
        }
      }
      // We fallback to sending empty heartbeat if job status check fails because
      // we don't want to trigger unnecessarily rebalance.
      // This ensures that job assignment is stable but there may be no progress on the job.
      // The master is responsible for ensuring job progress by checking job status progress.
      if (jobList == null) {
        jobList = ImmutableList.of();
      }

      if (controllerClient.getChannel().isTerminated()) {
        throw new IllegalStateException("master client grpc change is terminated");
      }
      // TODO(T4576995) re-resolve uns on heartbeatFailure
      // Send Heartbeat Request
      boolean heartbeatFailure = false;
      Stopwatch heartbeatTimer = infra.scope().timer(MetricsNames.HEARTBEAT_LATENCY).start();
      HeartbeatResponse heartbeatResponse = null;
      try {
        HeartbeatRequest heartbeatRequest =
            HeartbeatRequest.newBuilder()
                .setParticipants(
                    Participants.newBuilder()
                        .setMaster(controllerClient.getNode())
                        .setWorker(worker)
                        .build())
                .addAllJobStatus(jobList)
                .build();
        heartbeatResponse =
            controllerClient
                .getStub()
                .withDeadlineAfter(heartbeatTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .heartbeat(heartbeatRequest);
      } catch (Exception e) {
        heartbeatFailure = true;
        infra.scope().counter(MetricsNames.HEARTBEAT_FAILURE).inc(1);
        // Rethrow will log in the method's outer exception catch and ensure correct state
        // transition so we don't log here.
        throw e;
      } finally {
        heartbeatTimer.stop();
        if (!heartbeatFailure) {
          infra.scope().counter(MetricsNames.HEARTBEAT_SUCCESS).inc(1);
        }
      }
      if (heartbeatResponse.getParticipants().getMaster().equals(controllerClient.getNode())) {
        assertValidWorker(heartbeatResponse.getParticipants().getWorker().getId());
        lease.success();
        // Submit commands to user provided JobManager
        for (Command command : heartbeatResponse.getCommandsList()) {
          Function<Job, CompletionStage<Void>> controllableToRun = null;
          final Scope commandScope =
              infra
                  .scope()
                  .tagged(
                      ImmutableMap.of(
                          StructuredLogging.COMMAND_TYPE, command.getType().toString()));
          switch (command.getType()) {
            case COMMAND_TYPE_RUN_JOB:
              controllableToRun = controllable::run;
              break;
            case COMMAND_TYPE_CANCEL_JOB:
              controllableToRun = controllable::cancel;
              break;
            case COMMAND_TYPE_UPDATE_JOB:
              controllableToRun = controllable::update;
              break;
            default:
              logger.warn(
                  Controllable.Metrics.SKIP, StructuredLogging.commandType(command.getType()));
              commandScope.counter(Controllable.Metrics.SKIP).inc(1);
              continue;
          }

          // Execute controllable on a separate thread so we don't block the heartbeat.
          final Function<Job, CompletionStage<Void>> immutableControllableToRun = controllableToRun;
          final long workerId = heartbeatResponse.getParticipants().getWorker().getId();
          final Stopwatch commandTimer = commandScope.timer(Controllable.Metrics.LATENCY).start();
          commandScope.counter(Controllable.Metrics.CALLED).inc(1);
          logger.info(
              Controllable.Metrics.CALLED,
              StructuredLogging.workerId(workerId),
              StructuredLogging.jobId(command.getJob().getJobId()),
              StructuredLogging.kafkaTopic(command.getJob().getKafkaConsumerTask().getTopic()),
              StructuredLogging.kafkaCluster(command.getJob().getKafkaConsumerTask().getCluster()),
              StructuredLogging.kafkaGroup(
                  command.getJob().getKafkaConsumerTask().getConsumerGroup()),
              StructuredLogging.kafkaPartition(
                  command.getJob().getKafkaConsumerTask().getPartition()),
              StructuredLogging.commandType(command.getType().toString()));
          // Command timeout indicates PipelineManager failed to process command in limited time
          // This metric will be used as indicator of zombie worker
          ScheduledFuture commandTimeout =
              COMMAND_TIMEOUT_SCHEDULER.schedule(
                  () -> {
                    logger.error(
                        Controllable.Metrics.TIMEOUT,
                        StructuredLogging.workerId(workerId),
                        StructuredLogging.jobId(command.getJob().getJobId()),
                        StructuredLogging.kafkaTopic(
                            command.getJob().getKafkaConsumerTask().getTopic()),
                        StructuredLogging.kafkaCluster(
                            command.getJob().getKafkaConsumerTask().getCluster()),
                        StructuredLogging.kafkaGroup(
                            command.getJob().getKafkaConsumerTask().getConsumerGroup()),
                        StructuredLogging.kafkaPartition(
                            command.getJob().getKafkaConsumerTask().getPartition()),
                        StructuredLogging.commandType(command.getType().toString()));
                    infra.scope().counter(Controllable.Metrics.TIMEOUT).inc(1);
                  },
                  COMMAND_TIMEOUT_MS,
                  TimeUnit.MILLISECONDS);
          CompletableFuture.runAsync(
              () -> {
                immutableControllableToRun
                    .apply(command.getJob())
                    .whenComplete(
                        (v, ex) -> {
                          // cancel commandTimeout if complete timely
                          commandTimeout.cancel(false);
                          commandTimer.stop();
                          // TODO: this section is not covered due to flaky tests. Fix the flaky
                          // test.
                          if (ex != null) {
                            logger.warn(
                                Controllable.Metrics.FAILURE,
                                StructuredLogging.workerId(workerId),
                                StructuredLogging.jobId(command.getJob().getJobId()),
                                StructuredLogging.kafkaTopic(
                                    command.getJob().getKafkaConsumerTask().getTopic()),
                                StructuredLogging.kafkaCluster(
                                    command.getJob().getKafkaConsumerTask().getCluster()),
                                StructuredLogging.kafkaGroup(
                                    command.getJob().getKafkaConsumerTask().getConsumerGroup()),
                                StructuredLogging.kafkaPartition(
                                    command.getJob().getKafkaConsumerTask().getPartition()),
                                StructuredLogging.commandType(command.getType().toString()),
                                ex);
                            commandScope.counter(Controllable.Metrics.FAILURE).inc(1);
                          } else {
                            logger.info(
                                Controllable.Metrics.SUCCESS,
                                StructuredLogging.workerId(workerId),
                                StructuredLogging.jobId(command.getJob().getJobId()),
                                StructuredLogging.kafkaTopic(
                                    command.getJob().getKafkaConsumerTask().getTopic()),
                                StructuredLogging.kafkaCluster(
                                    command.getJob().getKafkaConsumerTask().getCluster()),
                                StructuredLogging.kafkaGroup(
                                    command.getJob().getKafkaConsumerTask().getConsumerGroup()),
                                StructuredLogging.kafkaPartition(
                                    command.getJob().getKafkaConsumerTask().getPartition()),
                                StructuredLogging.commandType(command.getType().toString()));
                            commandScope.counter(Controllable.Metrics.SUCCESS).inc(1);
                          }
                        });
              },
              commandExecutorService);
        }
        return StateWorking.from(
            this, heartbeatResponse.getParticipants().getWorker(), controllerClient);
      } else {
        // The else block handles the case that master changes.
        // skipping markSuccess
        isFailure = true;
        infra
            .scope()
            .tagged(
                ImmutableMap.of(
                    StructuredLogging.FROM_MASTER_HOST,
                    controllerClient.getNode().getHost(),
                    StructuredLogging.TO_MASTER_HOST,
                    heartbeatResponse.getParticipants().getMaster().getHost()))
            .counter(MetricsNames.HEARTBEAT_REDIRECT)
            .inc(1);
        ControllerClient newControllerClient = null;
        try {
          newControllerClient =
              controllerClientFactory.reconnectOnChange(
                  this.controllerClient, heartbeatResponse.getParticipants().getMaster());
        } catch (Exception reconnectException) {
          logger.error(
              String.format(
                  "[%s -> %s] "
                      + "got redirect master response but failed to reconnect to the new master",
                  StateWorking.STATE, StateWorking.STATE),
              StructuredLogging.masterHostPort(
                  NodeUtils.getHostAndPortString(heartbeatResponse.getParticipants().getMaster())),
              reconnectException);
          // rethrow the exception
          throw reconnectException;
        }
        markSuccess(
            "successfully redirect master",
            StateWorking.STATE,
            StateWorking.STATE,
            Duration.between(startNs, System.nanoTime()),
            NodeUtils.getHostAndPortString(heartbeatResponse.getParticipants().getMaster()),
            true);
        // the master will only provide the new master but not the worker, so we need to reuse
        // the worker
        return StateWorking.from(this, this.worker, newControllerClient);
      }
    } catch (Exception e) {
      isFailure = true;
      if (!lease.isValid()) {
        markError(
            "failed to heartbeat with master and lease expired",
            StateWorking.STATE,
            StateConnecting.STATE,
            Duration.between(startNs, System.nanoTime()),
            NodeUtils.getHostAndPortString(controllerClient.getNode()),
            e);
        cancelAll(aggregatedStage, cancelAllCommand);
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
      } else {
        // reconnect when the current leader master is unavailable, this could be caused by
        // 1. the current leader master is dead.
        // 2. this is network issue.
        //
        // in this case, master leader redirect protocol does not work.
        if (e instanceof StatusRuntimeException
            && ((StatusRuntimeException) e).getStatus() == Status.UNAVAILABLE) {
          ControllerClient newControllerClient = null;
          try {
            newControllerClient = controllerClientFactory.reconnect(controllerClient);
          } catch (Exception reconnectException) {
            markError(
                "lease has not expired. failed to heartbeat with master and reconnect failed",
                StateWorking.STATE,
                StateWorking.STATE,
                Duration.between(startNs, System.nanoTime()),
                NodeUtils.getHostAndPortString(controllerClient.getNode()),
                reconnectException);
            return StateWorking.from(this, this.worker, controllerClient);
          }
          if (!newControllerClient.getNode().equals(controllerClient.getNode())) {
            infra
                .scope()
                .tagged(
                    ImmutableMap.of(
                        StructuredLogging.FROM_MASTER_HOST,
                        controllerClient.getNode().getHost(),
                        StructuredLogging.TO_MASTER_HOST,
                        newControllerClient.getNode().getHost()))
                .counter(MetricsNames.HEARTBEAT_REDIRECT)
                .inc(1);
            markWarn(
                "lease has not expired. failed to heartbeat with master but successfully reconnect to a new master",
                StateWorking.STATE,
                StateWorking.STATE,
                Duration.between(startNs, System.nanoTime()),
                NodeUtils.getHostAndPortString(newControllerClient.getNode()),
                e);
          } else {
            markWarn(
                "lease has not expired. failed to heartbeat with master but successfully reconnect to the same master",
                StateWorking.STATE,
                StateWorking.STATE,
                Duration.between(startNs, System.nanoTime()),
                NodeUtils.getHostAndPortString(newControllerClient.getNode()),
                e);
          }
          return StateWorking.from(this, this.worker, newControllerClient);
        }
        // Do not aggressively reconnect. The reason is that the heartbeat might randomly fail, in
        // which case the leader does not change. Even if the master leader changes, the master
        // leader redirect protocol can redirect the worker to the current master leader.
        //
        // If we try to reconnect aggressively, it takes  several rounds for the worker to
        // (1) connect to a random master first
        // (2) then the worker is redirected to the leader
        // (3) then the worker sets up connection with the same master.
        // This process normally takes a time, causing the worker to fail heartbeat with the master,
        // leading to the result that the worker cancels all its running job, and the master assigns
        // those jobs to a new worker. The job reassignment will introduce unnecessary E2E latency
        // for the message receiver services.
        markWarn(
            "lease has not expired. failed to heartbeat with master",
            StateWorking.STATE,
            StateWorking.STATE,
            Duration.between(startNs, System.nanoTime()),
            NodeUtils.getHostAndPortString(controllerClient.getNode()),
            e);

        return StateWorking.from(this, this.worker, controllerClient);
      }
    } finally {
      if (!isFailure) {
        markSuccess(
            "successfully heartbeat with master",
            StateWorking.STATE,
            StateWorking.STATE,
            Duration.between(startNs, System.nanoTime()),
            NodeUtils.getHostAndPortString(controllerClient.getNode()),
            true);
      }
    }
  }

  private void cancelAll(String aggregatedStage, String cancelAllCommand) {
    Map<String, String> cancelAllTags =
        ImmutableMap.of(StructuredLogging.CONTROLLABLE_COMMAND, cancelAllCommand);
    infra.scope().tagged(cancelAllTags).counter(Controllable.Metrics.CALLED).inc(1);
    Stopwatch cancelAllTimer =
        infra.scope().tagged(cancelAllTags).timer(Controllable.Metrics.CALLED).start();
    controllable
        .cancelAll()
        .whenComplete(
            (v, ex) -> {
              cancelAllTimer.stop();
              if (ex != null) {
                logger.error(
                    Controllable.Metrics.FAILURE,
                    StructuredLogging.workerStage(aggregatedStage),
                    StructuredLogging.controllableCommand(cancelAllCommand),
                    ex);
                infra.scope().tagged(cancelAllTags).counter(Controllable.Metrics.FAILURE).inc(1);
              } else {
                logger.debug(
                    Controllable.Metrics.SUCCESS,
                    StructuredLogging.workerStage(aggregatedStage),
                    StructuredLogging.controllableCommand(cancelAllCommand));
                infra.scope().tagged(cancelAllTags).counter(Controllable.Metrics.SUCCESS).inc(1);
              }
            });
  }
}
