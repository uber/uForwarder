package com.uber.data.kafka.datatransfer.worker.common;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Controllable is an interface that the master-worker controller is able to interact with. The
 * Controller will use the Controllable interface to execute operations and collect feedback for the
 * proper functioning of the control plane.
 *
 * @implNote the default implementation is a noop Controller.
 * @implNote implementations should be threadsafe.
 */
public interface Controllable {

  /**
   * Runs a job.
   *
   * @param job to run.
   * @return CompletionStage that worker will use to log and metrics the success or failure of the
   *     call.
   * @implSpec exceptional completions will be log and metrics but will not be signal back to the
   *     master via the future. Rather, any failed job submission should be propagated back to the
   *     master via the returned {@code JobStatus}.
   */
  default CompletionStage<Void> run(Job job) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Cancels a job. Canceling a is equivalent to "stopping" a job. A job that is "canceled" should
   * not be run again.
   *
   * @param job to cancel.
   * @return CompletionStage that worker will use to log and metrics the success or failure of the
   *     call.
   * @implSpec exceptional completions will be log and metrics but will not be signal back to the
   *     master via the future. Rather, any failed job submission should be propagated back to the
   *     master via the returned {@code JobStatus}.
   */
  default CompletionStage<Void> cancel(Job job) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Updates a job for job configuration change. job.jobDefinition is not going to change in update
   * since it's guaranteed to be immutable
   *
   * @param job to update.
   * @return CompletionStage that worker will use to log and metrics the success or failure of the
   *     call.
   * @implSpec exceptional completions will be log and metrics but will not be signal back to the
   *     master via the future. Rather, any failed job submission should be propagated back to the
   *     master via the returned {@code JobStatus}.
   */
  default CompletionStage<Void> update(Job job) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Cancels all jobs that are running. This will be called when worker lease has expired so the
   * worker should stop all work because the master may have reassigned the work.
   *
   * @return CompletionStage that worker will use to log and metrics the success or failure of the
   *     call.
   */
  default CompletionStage<Void> cancelAll() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Gets all actual job status that this worker is actually working on.
   *
   * @return list of JobStatus for jobs that this worker is aware of.
   */
  default Collection<JobStatus> getJobStatus() {
    return ImmutableList.of();
  }

  /**
   * Gets all expected jobs that this worker is aware of. The worker should be converging to these.
   *
   * @return list of Job that this worker is aware of.
   */
  default Collection<Job> getJobs() {
    return ImmutableList.of();
  }

  final class Metrics {
    public static final String LATENCY = "controllable.latency";
    public static final String SUCCESS = "controllable.success";
    public static final String FAILURE = "controllable.failure";
    public static final String SKIP = "controllable.skip";
    public static final String CALLED = "controllable.called";
    public static final String TIMEOUT = "controllable.cmdTimeout";

    private Metrics() {}
  }

  final class Method {
    public static final String RUN = "run";
    public static final String CANCEL = "cancel";
    public static final String UPDATE = "update";
    public static final String CANCEL_ALL = "cancelall";
    public static final String GET_ALL = "getall";

    private Method() {}
  }
}
