package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.common.RoutingUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/** PipelineManager is an implementation of Controllable that manages a group of pipelines. */
public final class PipelineManager implements Controllable, MetricSource {

  private static final Logger logger = LoggerFactory.getLogger(PipelineManager.class);

  // stores the current running pipeline map
  private final ConcurrentMap<String, Pipeline> runningPipelineMap;
  // use pipeline as key in case that two pipeline has the same id.
  private final Map<Pipeline, String> gcPipelineMap;

  private final Scope scope;
  private final PipelineFactory pipelineFactory;

  PipelineManager(Scope scope, PipelineFactory pipelineFactory) {
    this.scope = scope.subScope("manager.pipeline");
    this.pipelineFactory = pipelineFactory;
    this.runningPipelineMap = new ConcurrentHashMap<>();
    this.gcPipelineMap = new HashMap<>();
  }

  public Map<String, Pipeline> getPipelines() {
    return runningPipelineMap;
  }

  private CompletionStage<Void> executeCallable(
      Function<Job, CompletionStage<Void>> callable,
      @Nullable Job job,
      String pipelineId,
      String command) {
    Scope fnScope =
        scope.tagged(
            ImmutableMap.of(
                StructuredLogging.CONTROLLABLE_COMMAND,
                command,
                StructuredLogging.PIPELINE_ID,
                pipelineId));
    fnScope.counter(Controllable.Metrics.CALLED).inc(1);
    Stopwatch timer = fnScope.timer(Controllable.Metrics.LATENCY).start();
    return callable
        .apply(job)
        .handle(
            (r, t) -> {
              timer.stop();
              if (t != null) {
                logger.warn(
                    Controllable.Metrics.FAILURE,
                    StructuredLogging.pipelineId(pipelineId),
                    StructuredLogging.controllableCommand(command),
                    t);
                fnScope.counter(Controllable.Metrics.FAILURE).inc(1);
                throw new CompletionException(t);
              } else {
                logger.debug(
                    Controllable.Metrics.SUCCESS,
                    StructuredLogging.pipelineId(pipelineId),
                    StructuredLogging.controllableCommand(command));
                scope.counter(Controllable.Metrics.SUCCESS).inc(1);
                return r;
              }
            });
  }

  private CompletionStage<Void> executeCallable(
      Supplier<CompletionStage<Void>> callable, String pipelineId, String command) {
    return executeCallable(job -> callable.get(), null, pipelineId, command);
  }

  private synchronized @Nullable Pipeline getPipeline(String pipelineId) {
    return runningPipelineMap.get(pipelineId);
  }

  @VisibleForTesting
  synchronized Pipeline getOrCreatePipeline(String pipelineId, Job job) {
    boolean isFailure = false;
    Scope fnScope = scope.tagged(ImmutableMap.of(StructuredLogging.PIPELINE_ID, pipelineId));
    Stopwatch getOrCreateTimer = fnScope.timer(Metrics.GET_OR_CREATE_LATENCY).start();
    Pipeline pipeline = getPipeline(pipelineId);
    // pipeline in runningPipelineMap should be running.
    if (pipeline != null) {
      getOrCreateTimer.stop();
      fnScope.counter(Metrics.GET_OR_CREATE_SUCCESS).inc(1);
      logger.debug(Logging.GET_OR_CREATE_SUCCESS);
      return pipeline;
    }
    // We need to lock before read-write block b/c concurrent map guarantees atomic map insertion
    // but
    // does not guarantee exactly create is invoked exactly once.
    // For simplicity (https://www.ibm.com/developerworks/library/j-jtp10264/index.html),
    // we synchronize the whole method.
    try {
      // Pipeline has not been created and we have lock so it is safe to create pipeline.
      Stopwatch createTimer = fnScope.timer(Metrics.CREATE_LATENCY).start();
      pipeline = pipelineFactory.createPipeline(pipelineId, job);
      pipeline.start();
      runningPipelineMap.put(pipelineId, pipeline);
      createTimer.stop();
      return pipeline;
    } catch (Exception e) {
      isFailure = true;
      fnScope.counter(Metrics.GET_OR_CREATE_FAILURE).inc(1);
      logger.warn(Logging.GET_OR_CREATE_FAILURE, StructuredLogging.pipelineId(pipelineId), e);
      throw new RuntimeException(e);
    } finally {
      getOrCreateTimer.stop();
      if (!isFailure) {
        logger.debug(Logging.GET_OR_CREATE_SUCCESS);
        fnScope.counter(Metrics.GET_OR_CREATE_SUCCESS).inc(1);
      }
    }
  }

  @Override
  public CompletionStage<Void> run(Job job) {
    String pipelineId = pipelineFactory.getPipelineId(job);
    Pipeline pipeline = getOrCreatePipeline(pipelineId, job);
    return executeCallable(pipeline::run, job, pipelineId, Controllable.Method.RUN);
  }

  @Override
  public CompletionStage<Void> cancel(Job job) {
    String pipelineId = pipelineFactory.getPipelineId(job);
    Pipeline pipeline = getPipeline(pipelineId);
    if (pipeline != null) {
      return executeCallable(pipeline::cancel, job, pipelineId, Controllable.Method.CANCEL);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletionStage<Void> update(Job job) {
    String pipelineId = pipelineFactory.getPipelineId(job);
    Pipeline pipeline = getPipeline(pipelineId);
    if (pipeline != null) {
      return executeCallable(pipeline::update, job, pipelineId, Method.UPDATE);
    } else {
      CompletableFuture<Void> completableFuture = new CompletableFuture<>();
      completableFuture.completeExceptionally(
          new Exception("the job is not assigned to this worker"));
      return completableFuture;
    }
  }

  @Override
  public synchronized CompletionStage<Void> cancelAll() {
    CompletableFuture<Void>[] futures = new CompletableFuture[runningPipelineMap.size()];
    int i = 0;
    final Scope fnScope =
        scope.tagged(
            ImmutableMap.of(
                StructuredLogging.CONTROLLABLE_COMMAND,
                Controllable.Method.CANCEL_ALL,
                StructuredLogging.PIPELINE_ID,
                "aggregated"));
    Stopwatch timer = fnScope.timer(Controllable.Metrics.LATENCY).start();
    for (Map.Entry<String, Pipeline> entry : runningPipelineMap.entrySet()) {
      futures[i++] =
          executeCallable(
                  entry.getValue()::cancelAll, entry.getKey(), Controllable.Method.CANCEL_ALL)
              .toCompletableFuture();
    }
    return CompletableFuture.allOf(futures)
        .whenComplete(
            (s, t) -> {
              timer.stop();
              if (t != null) {
                fnScope.counter(Controllable.Metrics.FAILURE).inc(1);
              } else {
                fnScope.counter(Controllable.Metrics.SUCCESS).inc(1);
              }
            });
  }

  /** @implNote do not synchronize getAll b/c weakly consistent getAll for heartbeat is OK. */
  @Override
  public Collection<JobStatus> getJobStatus() {
    ImmutableList.Builder<JobStatus> aggregatedJobStatusBuilder = ImmutableList.builder();
    Scope fnScope =
        scope.tagged(
            ImmutableMap.of(
                StructuredLogging.CONTROLLABLE_COMMAND,
                Controllable.Method.GET_ALL,
                StructuredLogging.PIPELINE_ID,
                "aggregated"));
    Stopwatch timer = fnScope.timer(Controllable.Metrics.LATENCY).start();
    boolean isFailure = false;
    try {
      for (Map.Entry<String, Pipeline> entry : runningPipelineMap.entrySet()) {
        aggregatedJobStatusBuilder.addAll(entry.getValue().getJobStatus());
      }
    } catch (Exception e) {
      isFailure = true;
      fnScope.counter(Controllable.Metrics.FAILURE).inc(1);
      logger.warn(
          Controllable.Metrics.FAILURE,
          StructuredLogging.pipelineId("aggregated"),
          StructuredLogging.controllableCommand(Controllable.Method.GET_ALL),
          e);
    } finally {
      timer.stop();
      if (!isFailure) {
        logger.debug(
            Controllable.Metrics.SUCCESS,
            StructuredLogging.pipelineId("aggregated"),
            StructuredLogging.controllableCommand(Controllable.Method.GET_ALL));
        fnScope.counter(Controllable.Metrics.SUCCESS).inc(1);
      }
    }
    return aggregatedJobStatusBuilder.build();
  }

  @Scheduled(fixedDelayString = "${worker.pipeline.manager.metricsInterval}")
  public void logAndMetrics() {
    // emit metric for number of actual job status in each job state.
    runningPipelineMap
        .values()
        .stream()
        .flatMap(p -> p.getJobStatus().stream())
        .map(js -> js.getState())
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
        .forEach(
            (jobState, count) -> {
              scope
                  .tagged(ImmutableMap.of("state", jobState.toString()))
                  .gauge("jobstatus.state.count")
                  .update(count);
              logger.debug(
                  "jobstatus.state.count",
                  StructuredLogging.count(count),
                  StructuredLogging.jobState(jobState));
            });

    // emit metric for number of expected job in each job state.
    long count = runningPipelineMap.values().stream().flatMap(p -> p.getJobs().stream()).count();
    scope
        .tagged(ImmutableMap.of("state", JobState.JOB_STATE_RUNNING.toString()))
        .gauge("job.state.count")
        .update(count);
    logger.debug(
        "job.state.count",
        StructuredLogging.count(count),
        StructuredLogging.jobState(JobState.JOB_STATE_RUNNING));

    // emit number of pipelines.
    logger.debug("running.count", StructuredLogging.count(runningPipelineMap.size()));
    scope.gauge("running.count").update(runningPipelineMap.size());
    logger.debug("gc.count", StructuredLogging.count(gcPipelineMap.size()));
    scope.gauge("gc.count").update(gcPipelineMap.size());
  }

  /**
   * Garbage collect stale pipelines by:
   *
   * <ol>
   *   <li>Close pipelines with all canceled jobs
   *   <li>Remove pipelines that are closed
   * </ol>
   *
   * @implNote we don't acquire locks for most of the gc operation so it is possible that there is a
   *     race condition between garbage collection and registering new running jobs. That is okay
   *     because when a pipeline is closed and removed, it will not register any running JobStatus
   *     in the worker to master heartbeat requests. When master detects that a worker is not
   *     performing expected work, it will reissue the RUN command to the worker so within (1-2
   *     heartbeats) the assignment should stabilize.
   */
  @Scheduled(
    fixedDelayString = "${worker.pipeline.manager.gcInterval}",
    initialDelayString = "${worker.pipeline.manager.gcInitialDelay:0}"
  )
  public void gcPipelines() {
    int removedCount = 0;
    int closedCount = 0;

    // only synchronized on the runningPipelineMap modification, because the actual pipeline to gc
    // can be
    // placed outside the lock
    synchronized (this) {
      // step 1: put pipelines that does not have running jobs into gcPipelineMap.
      Map<String, Pipeline> stillRunningPipelineMap = new HashMap<>();
      for (Map.Entry<String, Pipeline> entry : runningPipelineMap.entrySet()) {
        String pipelineId = entry.getKey();
        Pipeline pipeline = entry.getValue();
        // It is possible that when there is no expected job, job status cannot be changed to cancel
        // state, so we need to check
        // (1) all jobs are canceled
        // (2) there is no expected job.
        //
        // For example, in Kafka Consumer Proxy case, a processor might block the fetcher from
        // changing job status when the prefetch cache is full and the receivers are dead.
        if ((!pipeline.isRunning())
            || allJobsCanceled(pipeline.getJobStatus())
            || pipeline.getJobs().isEmpty()) {
          gcPipelineMap.put(pipeline, pipelineId);
        } else {
          stillRunningPipelineMap.put(pipelineId, pipeline);
        }
      }
      runningPipelineMap.clear();
      runningPipelineMap.putAll(stillRunningPipelineMap);
      stillRunningPipelineMap.clear();
    }

    // step 2: try to close pipelines and get stopped pipelines.
    Set<Pipeline> pipelineToDelete = new HashSet<>();
    for (Map.Entry<Pipeline, String> entry : gcPipelineMap.entrySet()) {
      String pipelineId = entry.getValue();
      Pipeline pipeline = entry.getKey();
      Stopwatch closeLatency =
          scope
              .tagged(ImmutableMap.of(StructuredLogging.PIPELINE_ID, pipelineId))
              .timer(Metrics.CLOSE_LATENCY)
              .start();
      boolean closeFailure = false;

      // Keep stopping running pipelines until they are not running any more.
      // Do not stop and remove a pipeline immediately because if it is not stopped successfully, we
      // can never reach it again, and it still occupies resources.
      if (pipeline.isRunning()) {
        try {
          pipeline.stop();
        } catch (Exception e) {
          closeFailure = true;
          scope
              .tagged(ImmutableMap.of(StructuredLogging.PIPELINE_ID, pipelineId))
              .counter(Metrics.CLOSE_FAILURE)
              .inc(1);
          logger.warn(Logging.CLOSE_FAILURE, StructuredLogging.pipelineId(pipelineId), e);
        } finally {
          closeLatency.stop();
          if (!closeFailure) {
            closedCount++;
            scope
                .tagged(ImmutableMap.of(StructuredLogging.PIPELINE_ID, pipelineId))
                .counter(Metrics.CLOSE_SUCCESS)
                .inc(1);
            logger.debug(Logging.CLOSE_SUCCESS, StructuredLogging.pipelineId(pipelineId));
          }
        }
      }
      if (!pipeline.isRunning()) {
        pipelineToDelete.add(pipeline);
      }
    }

    // step 3: remove stopped pipelines
    for (Pipeline pipeline : pipelineToDelete) {
      gcPipelineMap.remove(pipeline);
      removedCount++;
    }
    pipelineToDelete.clear();

    logger.debug(
        Logging.REMOVE_SUCCESS,
        StructuredLogging.pipelineId("aggregated"),
        StructuredLogging.count(removedCount));
    scope
        .tagged(ImmutableMap.of(StructuredLogging.PIPELINE_ID, "aggregated"))
        .counter(Metrics.REMOVE_SUCCESS)
        .inc(removedCount);

    logger.debug(
        Logging.CLOSE_SUCCESS,
        StructuredLogging.pipelineId("aggregated"),
        StructuredLogging.count(closedCount));
    scope
        .tagged(ImmutableMap.of(StructuredLogging.PIPELINE_ID, "aggregated"))
        .counter(Metrics.CLOSE_SUCCESS)
        .inc(closedCount);
  }

  private static boolean allJobsCanceled(Collection<JobStatus> jobs) {
    for (JobStatus job : jobs) {
      if (job.getState() != JobState.JOB_STATE_CANCELED) {
        return false;
      }
    }
    return true;
  }

  private void publishJobHostMap() {
    for (JobStatus jobStatus : getJobStatus()) {
      final String group = jobStatus.getJob().getKafkaConsumerTask().getConsumerGroup();
      final String cluster = jobStatus.getJob().getKafkaConsumerTask().getCluster();
      final String topic = jobStatus.getJob().getKafkaConsumerTask().getTopic();
      final String partition =
          Integer.toString(jobStatus.getJob().getKafkaConsumerTask().getPartition());
      final String routingKey =
          RoutingUtils.extractAddress(jobStatus.getJob().getRpcDispatcherTask().getUri());
      scope
          .tagged(
              ImmutableMap.of(
                  StructuredFields.KAFKA_GROUP,
                  group,
                  StructuredFields.KAFKA_CLUSTER,
                  cluster,
                  StructuredFields.KAFKA_TOPIC,
                  topic,
                  StructuredFields.KAFKA_PARTITION,
                  partition,
                  StructuredFields.URI,
                  routingKey))
          .gauge("job.host")
          .update(1.0);
    }
  }

  @Override
  public void publishMetrics() {
    runningPipelineMap.values().stream().forEach(pipeline -> pipeline.publishMetrics());
    publishJobHostMap();
  }

  static class Metrics {
    static final String CREATE_LATENCY = "create.latency";

    static final String GET_OR_CREATE_LATENCY = "getorcreate.latency";
    static final String GET_OR_CREATE_SUCCESS = "getorcreate.success";
    static final String GET_OR_CREATE_FAILURE = "getorcreate.failure";

    static final String CLOSE_LATENCY = "close.latency";
    static final String CLOSE_SUCCESS = "close.success";
    static final String CLOSE_FAILURE = "close.failure";

    static final String REMOVE_LATENCY = "remove.latency";
    static final String REMOVE_SUCCESS = "remove.success";
    static final String REMOVE_FAILURE = "remove.failure";

    static final String JOB_STATE_COUNT = "state.job.count";
  }

  static class Logging {
    static final String GET_OR_CREATE_SUCCESS = "pipeline.getorcreate.success";
    static final String GET_OR_CREATE_FAILURE = "pipeline.getorcreate.failure";

    static final String CLOSE_SUCCESS = "pipeline.close.success";
    static final String CLOSE_FAILURE = "pipeline.close.failure";

    static final String REMOVE_SUCCESS = "pipeline.remove.success";
    static final String REMOVE_FAILURE = "pipeline.remove.failure";
  }
}
