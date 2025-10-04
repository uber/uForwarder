package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.ScaleStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Throughput;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RebalancingJobGroup is a mutable view of a Job Group that represents a rebalancing computation.
 */
public final class RebalancingJobGroup {
  private static final double EPSILON = 0.00001d;
  private static final Logger logger = LoggerFactory.getLogger(RebalancingJobGroup.class);
  private volatile StoredJobGroup storedJobGroup;
  private final ConcurrentMap<Long, StoredJob> jobsMap;
  private final Map<Long, StoredJobStatus> jobStatusMap;
  private final int version;
  private AtomicBoolean changed;

  private RebalancingJobGroup(
      StoredJobGroup storedJobGroup,
      int version,
      ConcurrentMap<Long, StoredJob> jobsMap,
      Map<Long, StoredJobStatus> jobStatusMap) {
    this.storedJobGroup = storedJobGroup;
    this.version = version;
    this.jobsMap = jobsMap;
    this.jobStatusMap = Collections.unmodifiableMap(jobStatusMap);
    this.changed = new AtomicBoolean(false);
  }

  /** Creates a new RebalancingJobGroup for a stored job group. */
  public static RebalancingJobGroup of(
      Versioned<StoredJobGroup> storedJobGroup, Map<Long, StoredJobStatus> jobStatusMap) {
    return new RebalancingJobGroup(
        storedJobGroup.model(),
        storedJobGroup.version(),
        // this map is deliberately generated to be mutable.
        storedJobGroup.model().getJobsList().stream()
            .collect(Collectors.toConcurrentMap(k -> k.getJob().getJobId(), v -> v)),
        jobStatusMap);
  }

  /** Returns true if the computation has changed from the initialized object. */
  public synchronized boolean isChanged() {
    return changed.get();
  }

  /**
   * Gets the {@link JobGroup}.
   *
   * @return an immutable JobGroup.
   */
  public synchronized JobGroup getJobGroup() {
    return storedJobGroup.getJobGroup();
  }

  /**
   * Gets the {@link JobState} for the job group.
   *
   * @return an immutable JobState.
   */
  public synchronized JobState getJobGroupState() {
    return storedJobGroup.getState();
  }

  /**
   * Gets scale of the job group
   *
   * @return the scale
   */
  public synchronized Optional<Double> getScale() {
    return storedJobGroup.hasScaleStatus()
        ? Optional.of(storedJobGroup.getScaleStatus().getScale())
        : Optional.empty();
  }

  /**
   * Gets the throughput of the job group
   *
   * @return the throughput
   */
  public synchronized Optional<Throughput> getThroughput() {
    return storedJobGroup.hasScaleStatus()
        ? Optional.of(
            new Throughput(
                storedJobGroup.getScaleStatus().getTotalMessagesPerSec(),
                storedJobGroup.getScaleStatus().getTotalBytesPerSec()))
        : Optional.empty();
  }

  /**
   * Gets the {@link StoredJob} that are currently stored in the job group.
   *
   * @return an immutable map of jobs.
   */
  public synchronized Map<Long, StoredJob> getJobs() {
    return Collections.unmodifiableMap(jobsMap);
  }

  /**
   * Gets the {@link StoredJobStatus} for each job in this {@link JobGroup}.
   *
   * @return an immutable map of job statuses.
   */
  public synchronized Map<Long, StoredJobStatus> getJobStatusMap() {
    return jobStatusMap;
  }

  /**
   * Updates the {@link StoredJob} job.
   *
   * @return true if updateJob was successful and it changed the value, otherwise false.
   *     <p>Update only applies the change if the job already exists. You cannot use this method to
   *     add a new job that did not previously exist.
   */
  public synchronized boolean updateJob(long jobId, StoredJob job) {
    AtomicBoolean updated = new AtomicBoolean(false);
    jobsMap.computeIfPresent(
        jobId,
        (id, oldJob) -> {
          if (!oldJob.equals(job)) {
            updated.set(true);
            changed.set(true);
          }
          if (oldJob.getWorkerId() != job.getWorkerId()) {
            logger.debug(
                "update worker id in job",
                StructuredLogging.jobGroupId(storedJobGroup.getJobGroup().getJobGroupId()),
                StructuredLogging.jobId(jobId),
                StructuredLogging.fromId(oldJob.getWorkerId()),
                StructuredLogging.toId(job.getWorkerId()));
          }
          if (oldJob.getState() != job.getState()) {
            logger.debug(
                "update job state in job",
                StructuredLogging.jobGroupId(storedJobGroup.getJobGroup().getJobGroupId()),
                StructuredLogging.jobId(jobId),
                StructuredLogging.fromState(oldJob.getState().toString()),
                StructuredLogging.toState(job.getState().toString()));
          }
          return job;
        });
    return updated.get();
  }

  /**
   * Updates the Job Group State.
   *
   * @return true if update was successful and value was changed, otherwise false.
   */
  public synchronized boolean updateJobGroupState(JobState jobGroupState) {
    JobState oldState = storedJobGroup.getState();
    if (oldState == jobGroupState) {
      return false;
    }
    if (oldState != jobGroupState) {
      logger.info(
          "update job group state",
          StructuredLogging.fromState(oldState.toString()),
          StructuredLogging.toState(jobGroupState.toString()),
          StructuredLogging.jobGroupId(storedJobGroup.getJobGroup().getJobGroupId()));
    }
    storedJobGroup = storedJobGroup.toBuilder().setState(jobGroupState).build();
    changed.set(true);
    return true;
  }

  /**
   * Updates scale of the job group.
   *
   * @param newScale the new scale
   * @return the boolean
   */
  public synchronized boolean updateScale(double newScale, Throughput throughput) {
    if (storedJobGroup.hasScaleStatus() && newScale == storedJobGroup.getScaleStatus().getScale()) {
      return false;
    }
    ScaleStatus scaleStatus =
        ScaleStatus.newBuilder()
            .setScale(newScale)
            .setTotalMessagesPerSec(throughput.getMessagesPerSecond())
            .setTotalBytesPerSec(throughput.getBytesPerSecond())
            .build();
    storedJobGroup = StoredJobGroup.newBuilder(storedJobGroup).setScaleStatus(scaleStatus).build();
    changed.set(true);
    return true;
  }

  /** Returns a new StoredJobGroup that represents the (mutated) computation. */
  public Versioned<StoredJobGroup> toStoredJobGroup() {
    StoredJobGroup.Builder builder = storedJobGroup.toBuilder();
    builder.clearJobs();
    builder.addAllJobs(jobsMap.values());
    return Versioned.from(builder.build(), version);
  }

  /**
   * Returns a predicate that filters RebalancingJobGroup for job group state that matches the
   * provided input set.
   */
  public static Predicate<RebalancingJobGroup> filterByJobGroupState(final Set<JobState> states) {
    return rebalancingJobGroup -> states.contains(rebalancingJobGroup.getJobGroupState());
  }
}
