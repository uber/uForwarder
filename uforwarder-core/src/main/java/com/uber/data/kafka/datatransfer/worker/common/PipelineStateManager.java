package com.uber.data.kafka.datatransfer.worker.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * PipelineStateManager is a common interface used to manage DataTransferFramework {@link
 * com.uber.data.kafka.datatransfer.worker.pipelines.Pipeline} state.
 *
 * @implNote implementations should be threadsafe.
 */
public interface PipelineStateManager extends Controllable, MetricSource {

  /** Updates the {@link JobStatus}, i.e., the actual state. */
  default void updateActualRunningJobStatus(List<JobStatus> jobStatusList) {}

  /**
   * Returns the map of expected running <job ID, jobs>, i.e., the expected running state.
   *
   * <p>As job ID is the key of the map, we can guarantee that there will not be more than one job
   * having the same job ID.
   */
  default Map<Long, Job> getExpectedRunningJobMap() {
    return ImmutableMap.of();
  }

  /**
   * Determines whether a job is expected to be running or not.
   *
   * <p>A job is expected running when this exact job is in the map of expected running <job ID,
   * jobs>, but not its job ID is in the map. This avoids the case that when a job is replaced by a
   * job with the same job ID, we falsely believe that the job is still expected to be running.
   */
  default boolean shouldJobBeRunning(Job job) {
    return false;
  }

  /**
   * Gets the expected job (wrapped in optional) for the given job ID if it exists. Otherwise,
   * returns Optional.empty().
   *
   * @param jobId a job ID.
   */
  default Optional<Job> getExpectedJob(long jobId) {
    return Optional.empty();
  }

  /** Returns the job definition template representing all jobs. */
  default Job getJobTemplate() {
    return Job.newBuilder().build();
  }

  /** Returns the total flow control for a pipeline. */
  default FlowControl getFlowControl() {
    return FlowControl.newBuilder().build();
  }

  /** Clears all configurations and status. */
  default void clear() {}

  @Override
  default Collection<Job> getJobs() {
    return ImmutableList.copyOf(getExpectedRunningJobMap().values());
  }
}
