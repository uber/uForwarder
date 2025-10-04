package com.uber.data.kafka.datatransfer.controller.rpc;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.uber.data.kafka.datatransfer.Command;
import com.uber.data.kafka.datatransfer.HeartbeatRequest;
import com.uber.data.kafka.datatransfer.HeartbeatResponse;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskStatus;
import com.uber.data.kafka.datatransfer.MasterWorkerServiceGrpc;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.Participants;
import com.uber.data.kafka.datatransfer.RegisterWorkerRequest;
import com.uber.data.kafka.datatransfer.RegisterWorkerResponse;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.WorkerState;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.NodeUtils;
import com.uber.data.kafka.datatransfer.common.ReadStore;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.TimestampUtils;
import com.uber.data.kafka.datatransfer.common.VersionedProto;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.instrumentation.BiConsumerConverter;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.data.kafka.instrumentation.ThrowingBiConsumer;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InternalApi
public final class ControllerWorkerService
    extends MasterWorkerServiceGrpc.MasterWorkerServiceImplBase {

  private static final Logger logger = LoggerFactory.getLogger(ControllerWorkerService.class);
  private final Store<Long, StoredWorker> workerStore;
  private final ReadStore<Long, StoredJob> jobStore;
  private final Store<Long, StoredJobStatus> jobStatusStore;
  private final Node master;
  private final LeaderSelector leaderSelector;
  private final CoreInfra infra;
  private final JobWorkloadSink jobWorkloadSink;

  public ControllerWorkerService(
      CoreInfra infra,
      Node master,
      Store<Long, StoredWorker> workerStore,
      ReadStore<Long, StoredJob> jobStore,
      Store<Long, StoredJobStatus> jobStatusStore,
      LeaderSelector leaderSelector,
      JobWorkloadSink jobWorkloadSink) {
    this.infra = infra;
    this.master = master;
    this.workerStore = workerStore;
    this.jobStore = jobStore;
    this.jobStatusStore = jobStatusStore;
    this.leaderSelector = leaderSelector;
    this.jobWorkloadSink = jobWorkloadSink;
  }

  private <Req, Res, E extends Exception> BiConsumer<Req, StreamObserver<Res>> withLeaderRedirect(
      ThrowingBiConsumer<Req, StreamObserver<Res>, E> handler,
      Function<Node, Res> redirectResponseBuilder) {
    return (req, resW) -> {
      if (!leaderSelector.isLeader()) {
        Node currentLeader = NodeUtils.newNode(leaderSelector.getLeaderId());
        if (!master.equals(currentLeader)) {
          resW.onNext(redirectResponseBuilder.apply(currentLeader));
          resW.onCompleted();
          infra
              .scope()
              .tagged(ImmutableMap.of(StructuredLogging.TO_MASTER_HOST, currentLeader.getHost()))
              .counter("masterworkerservice.redirect")
              .inc(1);
          logger.info("masterworkerservice.redirect", "redirect-to", currentLeader.getHost());
          return;
        }
      }
      BiConsumerConverter.uncheck(handler).accept(req, resW);
    };
  }

  @Override
  public void registerWorker(
      RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> responseObserver) {
    Instrumentation.instrument.withStreamObserver(
        logger,
        infra.scope(),
        infra.tracer(),
        withLeaderRedirect(
            (req, resW) -> {
              instrumentParticipants(req.getParticipants().getWorker());
              Versioned<StoredWorker> worker =
                  workerStore.create(
                      WorkerUtils.newWorker(request.getParticipants().getWorker()),
                      WorkerUtils::withWorkerId);
              workerStore.put(
                  worker.model().getNode().getId(),
                  VersionedProto.from(
                      StoredWorker.newBuilder(worker.model())
                          .setState(WorkerState.WORKER_STATE_REGISTERING)
                          .build(),
                      worker.version()));
              resW.onNext(
                  RegisterWorkerResponse.newBuilder()
                      .setParticipants(
                          Participants.newBuilder()
                              .setMaster(master)
                              .setWorker(worker.model().getNode())
                              .build())
                      .build());
              resW.onCompleted();
            },
            // we deliberately don't set worker because the response should only contain
            // worker info got from the zookeeper.
            node ->
                RegisterWorkerResponse.newBuilder()
                    .setParticipants(Participants.newBuilder().setMaster(node).build())
                    .build()),
        request,
        responseObserver,
        "masterworkerservice.registerworker");
  }

  @Override
  public void heartbeat(
      HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
    Instrumentation.instrument.withStreamObserver(
        logger,
        infra.scope(),
        infra.tracer(),
        withLeaderRedirect(
            (req, resW) -> {
              instrumentParticipants(req.getParticipants().getWorker());
              Node node = request.getParticipants().getWorker();
              long workerId = node.getId();
              // If a worker does not exist, workerStore.get will throw an exception, and this
              // master will
              // send the exception to the worker.
              Versioned<StoredWorker> worker = workerStore.get(workerId);
              Map<Long, Versioned<StoredJob>> assigned =
                  jobStore.getAll(job -> job.getWorkerId() == worker.model().getNode().getId());
              // workers only report running jobs
              List<JobStatus> running = request.getJobStatusList();
              updateJobStatuses(running, workerId);
              List<Command> commandList =
                  buildCommandList(buildAssignedJobsMap(assigned), buildRunningJobsMap(running));
              logCommandsToWorker(workerId, commandList);

              // putThrough if state is changing
              if (worker.model().getState() != WorkerState.WORKER_STATE_WORKING) {
                workerStore.putThrough(
                    worker.model().getNode().getId(),
                    VersionedProto.from(
                        StoredWorker.newBuilder(worker.model())
                            .setState(WorkerState.WORKER_STATE_WORKING)
                            .build(),
                        worker.version()));
              } else {
                // otherwise, use regular put, which defers to the configured bufferedWriteInterval.
                workerStore.put(
                    worker.model().getNode().getId(),
                    VersionedProto.from(
                        StoredWorker.newBuilder(worker.model())
                            .setState(WorkerState.WORKER_STATE_WORKING)
                            .build(),
                        worker.version()));
              }
              resW.onNext(
                  HeartbeatResponse.newBuilder()
                      .setParticipants(
                          Participants.newBuilder()
                              .setMaster(master)
                              .setWorker(worker.model().getNode())
                              .build())
                      .addAllCommands(commandList)
                      .build());
              resW.onCompleted();
            },
            // we deliberately don't set worker because the response should only contain
            // worker info got from the zookeeper.
            node ->
                HeartbeatResponse.newBuilder()
                    .setParticipants(Participants.newBuilder().setMaster(node).build())
                    .build()),
        request,
        responseObserver,
        "masterworkerservice.heartbeat");
  }

  private void logCommandsToWorker(long workerId, List<Command> commandList) {
    commandList.forEach(
        c ->
            logger.info(
                "sending command to worker",
                StructuredLogging.workerId(workerId),
                StructuredLogging.jobId(c.getJob().getJobId()),
                StructuredLogging.kafkaTopic(c.getJob().getKafkaConsumerTask().getTopic()),
                StructuredLogging.kafkaCluster(c.getJob().getKafkaConsumerTask().getCluster()),
                StructuredLogging.kafkaGroup(c.getJob().getKafkaConsumerTask().getConsumerGroup()),
                StructuredLogging.kafkaPartition(c.getJob().getKafkaConsumerTask().getPartition()),
                StructuredLogging.commandType(c.getType().toString())));
  }

  private void instrumentParticipants(Node worker) {
    long workerId = worker.getId();
    if (workerId != WorkerUtils.UNSET_WORKER_ID) {
      infra.scope().counter("masterworkerservice.reconnect").inc(1);
      logger.debug("masterworkerservice.reconnect", StructuredLogging.workerHost(worker.getHost()));
    } else {
      infra.scope().counter("masterworkerservice.new").inc(1);
      logger.debug("masterworkerservice.new", StructuredLogging.workerHost(worker.getHost()));
    }
  }

  private static Map<Long, StoredJob> buildAssignedJobsMap(
      Map<Long, Versioned<StoredJob>> jobList) {
    ImmutableMap.Builder<Long, StoredJob> jobsMapBuilder = new ImmutableMap.Builder<>();
    for (Map.Entry<Long, Versioned<StoredJob>> entry : jobList.entrySet()) {
      jobsMapBuilder.put(entry.getKey(), entry.getValue().model());
    }
    return jobsMapBuilder.build();
  }

  private static Map<Long, JobStatus> buildRunningJobsMap(List<JobStatus> jobStatusList) {
    ImmutableMap.Builder<Long, JobStatus> jobsMapBuilder = new ImmutableMap.Builder<>();
    for (JobStatus job : jobStatusList) {
      jobsMapBuilder.put(job.getJob().getJobId(), job);
    }
    return jobsMapBuilder.build();
  }

  private List<Command> buildCommandList(
      Map<Long, StoredJob> expectedJob, Map<Long, JobStatus> actualJobs) throws Exception {
    return Instrumentation.instrument.withException(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          CommandListBuilder builder = new CommandListBuilder(infra.scope());

          Map<Long, JobStatus> actualJobsCopy = new HashMap<>(actualJobs);

          // iterate over expected jobs
          for (Map.Entry<Long, StoredJob> expected : expectedJob.entrySet()) {
            builder.add(
                expected.getKey(),
                expected.getValue(),
                actualJobsCopy.getOrDefault(expected.getKey(), JobStatus.newBuilder().build()));
            actualJobsCopy.remove(expected.getKey());
          }

          // iterate over running jobs and add necessary commands to command builder
          // b/c there may be an actual (running) job status that maps to a deleted expected job.
          // In these case, we need to issue CANCEL command.
          for (Map.Entry<Long, JobStatus> actual : actualJobsCopy.entrySet()) {
            builder.add(actual.getKey(), StoredJob.newBuilder().build(), actual.getValue());
          }

          return builder.build();
        },
        "masterworkerservice.buildcommandlist");
  }

  @VisibleForTesting
  void updateJobStatuses(List<JobStatus> jobStatusList, long workerId) {
    Instrumentation.instrument.returnVoidCatchThrowable(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> {
          Timestamp timestamp = TimestampUtils.currentTimeMilliseconds();
          // Job status updates are done in parallel using Java parallel stream
          jobStatusList.parallelStream()
              .forEach(
                  jobStatus -> {
                    KafkaConsumerTask task = jobStatus.getJob().getKafkaConsumerTask();
                    KafkaConsumerTaskStatus taskStatus = jobStatus.getKafkaConsumerTaskStatus();
                    jobWorkloadSink.consume(
                        jobStatus.getJob(),
                        Workload.of(
                            taskStatus.getMessagesPerSec(),
                            taskStatus.getBytesPerSec(),
                            taskStatus.getCpuUsage()));
                    StoredJobStatus storedJobStatus =
                        StoredJobStatus.newBuilder()
                            .setLastUpdated(timestamp)
                            .setJobStatus(jobStatus)
                            .setWorkerId(workerId)
                            .build();
                    infra
                        .scope()
                        .tagged(
                            ImmutableMap.of(
                                StructuredLogging.KAFKA_GROUP, task.getConsumerGroup(),
                                StructuredLogging.KAFKA_TOPIC, task.getTopic(),
                                StructuredLogging.KAFKA_PARTITION,
                                    Integer.toString(task.getPartition())))
                        .gauge(StructuredLogging.COMMITTED_OFFSET)
                        .update(taskStatus.getCommitOffset());
                    try {
                      // try to get the existing job status
                      Versioned<StoredJobStatus> oldStoredJobStatus =
                          jobStatusStore.get(jobStatus.getJob().getJobId());
                      StoredJobStatus oldJobStatusModel = oldStoredJobStatus.model();
                      // Check if the job status has been changed
                      boolean jobStatusNotChanged =
                          Objects.nonNull(oldJobStatusModel)
                              && jobStatus.equals(oldJobStatusModel.getJobStatus())
                              && workerId == oldJobStatusModel.getWorkerId();
                      boolean jobStatusChanged = !jobStatusNotChanged;
                      try {
                        // try to put job status to zk only if the job status is changed
                        if (jobStatusChanged) {
                          jobStatusStore.put(
                              jobStatus.getJob().getJobId(),
                              Versioned.from(storedJobStatus, oldStoredJobStatus.version()));
                        }
                      } catch (Exception e) {
                        logger.warn(
                            "failed to put stored jobStatus to zk",
                            StructuredLogging.jobId(jobStatus.getJob().getJobId()),
                            StructuredLogging.kafkaGroup(task.getConsumerGroup()),
                            StructuredLogging.kafkaTopic(task.getTopic()),
                            StructuredLogging.kafkaPartition(task.getPartition()),
                            e);
                      }
                    } catch (Exception e) {
                      logger.debug(
                          "no stored jobStatus before",
                          StructuredLogging.jobId(jobStatus.getJob().getJobId()),
                          e);
                      try {
                        // if no stored job status, try to create one
                        jobStatusStore.create(storedJobStatus, (key, value) -> value);
                      } catch (Exception ex) {
                        logger.error(
                            "failed to create stored jobStatus",
                            StructuredLogging.jobId(jobStatus.getJob().getJobId()),
                            StructuredLogging.kafkaGroup(task.getConsumerGroup()),
                            StructuredLogging.kafkaTopic(task.getTopic()),
                            StructuredLogging.kafkaPartition(task.getPartition()),
                            ex);
                      }
                    }
                  });
        },
        "masterworkerservice.updatejobstatuses");
  }
}
