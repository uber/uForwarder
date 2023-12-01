package com.uber.data.kafka.datatransfer.controller.manager;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.JobSnapshot;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.data.kafka.datatransfer.controller.rebalancer.ShadowRebalancerDelegate;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * JobManager manages job state for the master.
 *
 * <p>JobManager functionality is:
 *
 * <ol>
 *   <li>Ensuring that all jobs are assigned to valid live workers via {@code rebalanceJobGroups}
 *   <li>Emit aggregate metrics for # of jobs in each state via {@code logAndMetrics}
 * </ol>
 *
 * The {@code rebalanceJobGroups} algorithm is:
 *
 * <ol>
 *   <li>Get live workers from worker store
 *   <li>Get all job groups by querying job group store
 *   <li>Iterate over jobs in a job group and in-memory
 *       <ol>
 *         <li>Set worker_id to "0" if previously assigned to a dead worker
 *         <li>Call user provided {@code Rebalancer} to compute new placement
 *         <li>Write new placement returned from {@code Rebalancer} to a pending update map
 *       </ol>
 *   <li>Once all update for a single job group is computed, commit updates to storage via a single
 *       ZK write to the job group Znode.
 * </ol>
 */
@InternalApi
public final class JobManager {

  private static final Logger logger = LoggerFactory.getLogger(JobManager.class);

  private final Scope scope;
  private final Store<String, StoredJobGroup> jobGroupStore;
  private final Store<Long, StoredJobStatus> jobStatusStore;
  private final Store<Long, StoredWorker> workerStore;
  private final Rebalancer rebalancer;

  private final ShadowRebalancerDelegate shadowRebalancerDelegate;

  private final LeaderSelector leaderSelector;
  /**
   * JobManager constructor is visible for testing only.
   *
   * <p>External production code should use the provided {@code Builder}
   */
  JobManager(
      Scope scope,
      Store<String, StoredJobGroup> jobGroupStore,
      Store<Long, StoredJobStatus> jobStatusStore,
      Store<Long, StoredWorker> workerStore,
      Rebalancer rebalancer,
      ShadowRebalancerDelegate shadowRebalancerDelegate,
      LeaderSelector leaderSelector) {
    this.scope = scope;
    this.jobGroupStore = jobGroupStore;
    this.jobStatusStore = jobStatusStore;
    this.workerStore = workerStore;
    this.rebalancer = rebalancer;
    this.shadowRebalancerDelegate = shadowRebalancerDelegate;
    this.leaderSelector = leaderSelector;
  }

  /** Log and metrics periodic snapshot of job manager state * */
  @Scheduled(fixedDelayString = "${master.manager.job.metricsInterval}")
  public void logAndMetrics() {
    if (!leaderSelector.isLeader()) {
      logger.debug("skipped logAndMetrics because of current instance is not leader");
      return;
    }
    boolean isFailure = false;
    try {
      Map<Long, JobSnapshot> jobs =
          jobGroupStore
              .getAll()
              .values()
              .stream()
              .map(g -> g.model().getJobsList())
              .flatMap(Collection::stream)
              .collect(
                  Collectors.toMap(
                      k -> k.getJob().getJobId(),
                      v -> JobSnapshot.newBuilder().setExpectedJob(v).build()));
      Map<JobState, Integer> jobStateCountMap = new HashMap<>();
      for (JobSnapshot job : jobs.values()) {
        jobStateCountMap.merge(
            job.getExpectedJob().getState(), 1, (oldValue, newValue) -> oldValue + newValue);
      }
      for (Map.Entry<JobState, Integer> entry : jobStateCountMap.entrySet()) {
        scope
            .tagged(ImmutableMap.of("job_state", entry.getKey().name()))
            .gauge("manager.job.state.expected.count")
            .update((double) entry.getValue());
        logger.info(
            entry.getKey().toString() + " count",
            StructuredLogging.count(entry.getValue()),
            StructuredLogging.idealState(entry.getKey()));
      }
    } catch (Throwable t) {
      isFailure = true;
      logger.error("job manager log and metrics heartbeat failed", t);
      scope.counter("manager.job.heartbeat.failed").inc(1);
    } finally {
      if (!isFailure) {
        logger.debug("job manager log and metrics heartbeat success");
        scope.counter("manager.job.heartbeat.success").inc(1);
      }
    }
  }

  private static <K, V> Map<K, V> unversion(Map<K, Versioned<V>> versionedMap) {
    return Collections.unmodifiableMap(
        versionedMap
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().model())));
  }

  private Map<String, RebalancingJobGroup> generateRebalancingJobGroups(
      Map<String, Versioned<StoredJobGroup>> jobGroups, Map<Long, StoredJobStatus> jobStatusMap) {
    Map<String, RebalancingJobGroup> rebalancingJobGroupMap = new HashMap<>();
    for (Map.Entry<String, Versioned<StoredJobGroup>> jobGroupEntry : jobGroups.entrySet()) {
      Set<Long> jobIdsForJobGroup =
          jobGroupEntry
              .getValue()
              .model()
              .getJobsList()
              .stream()
              .map(j -> j.getJob().getJobId())
              .collect(Collectors.toSet());
      Map<Long, StoredJobStatus> jobStatusForJobGroup =
          jobStatusMap
              .entrySet()
              .stream()
              .filter(e -> jobIdsForJobGroup.contains(e.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      rebalancingJobGroupMap.put(
          jobGroupEntry.getKey(),
          RebalancingJobGroup.of(jobGroupEntry.getValue(), jobStatusForJobGroup));
    }
    return Collections.unmodifiableMap(rebalancingJobGroupMap);
  }

  private static void ensureValidWorkerId(
      Collection<RebalancingJobGroup> jobGroups, Set<Long> workerIds) {
    for (RebalancingJobGroup rebalancingJobGroup : jobGroups) {
      for (Map.Entry<Long, StoredJob> jobEntry : rebalancingJobGroup.getJobs().entrySet()) {
        long jobId = jobEntry.getKey();
        long workerId = jobEntry.getValue().getWorkerId();
        if (workerId != WorkerUtils.UNSET_WORKER_ID && !workerIds.contains(workerId)) {
          rebalancingJobGroup.updateJob(
              jobId,
              jobEntry.getValue().toBuilder().setWorkerId(WorkerUtils.UNSET_WORKER_ID).build());
        }
      }
    }
  }

  private void writeJobGroupStoreIfChanged(Map<String, RebalancingJobGroup> jobGroups)
      throws Exception {
    for (Map.Entry<String, RebalancingJobGroup> jobGroupEntry : jobGroups.entrySet()) {
      String jobGroupId = jobGroupEntry.getKey();
      RebalancingJobGroup rebalancingJobGroup = jobGroupEntry.getValue();
      if (rebalancingJobGroup.isChanged()) {
        jobGroupStore.put(jobGroupId, rebalancingJobGroup.toStoredJobGroup());
      }
    }
  }

  // Single threaded run is ensured by "fixedDelayString" instead of "fixedRateString", which
  // schedules
  // the next iteration only after the previous one completes.

  /**
   * Rebalances all job groups.
   *
   * @implNote we guarantee a single invocation of rebalanceJobGroups at any given time by
   *     using @Schedule with fixedDelayString.
   *     <p>TODO (T4575557): trigger rebalance based on "unbalanced work" as well as dead worker.
   */
  @Scheduled(fixedDelayString = "${master.manager.job.rebalanceInterval}")
  public void rebalanceJobGroups() throws Exception {
    if (!leaderSelector.isLeader()) {
      logger.debug("skipped rebalanceJobGroups because of current instance is not leader");
      return;
    }
    Stopwatch rebalanceJobGroupsTimer =
        scope.timer(MetricsNames.REBALANCE_JOB_GROUPS_LATENCY).start();
    boolean rebalanceJobGroupsFailure = false;
    try {
      // 1. Get all workers from worker store
      Map<Long, StoredWorker> workerMap = unversion(workerStore.getAll());

      // 2. Get all job groups as RebalancingJobGroup
      Map<String, Versioned<StoredJobGroup>> jobGroups = jobGroupStore.getAll();
      Map<Long, StoredJobStatus> jobStatus = unversion(jobStatusStore.getAll());

      // 3. Generate the rebalancing job groups
      Map<String, RebalancingJobGroup> rebalancingJobGroupMap =
          generateRebalancingJobGroups(jobGroups, jobStatus);

      // 4. compute load of each jobGroup / job
      rebalancer.computeLoad(rebalancingJobGroupMap);

      // 5. Call computeJobConfiguration
      rebalancer.computeJobConfiguration(rebalancingJobGroupMap, workerMap);

      // 6. Unset jobs assigned to stale workers.
      // We do this in the framework instead of delegating to each rebalancer impl
      // b/c this is important to ensure no zombie jobs.
      ensureValidWorkerId(rebalancingJobGroupMap.values(), workerMap.keySet());

      // 7. Call computeJobState
      // we change job state first so that canceled jobs will not be assigned to workers.
      rebalancer.computeJobState(rebalancingJobGroupMap, workerMap);

      // 8. Call computeWorkerId
      rebalancer.computeWorkerId(rebalancingJobGroupMap, workerMap);

      // 9. post process jobs
      rebalancer.postProcess(rebalancingJobGroupMap, workerMap);

      // 10. if changed, write new job group.
      writeJobGroupStoreIfChanged(rebalancingJobGroupMap);

      // 11. If assignment changed, clear all the job statuses. The next heartbeat from the workers
      // will populate them again.
      maybeClearJobStatuses(rebalancingJobGroupMap);
    } catch (Throwable t) {
      rebalanceJobGroupsFailure = true;
      scope.counter(MetricsNames.REBALANCE_JOB_GROUPS_FAILURE).inc(1);
      logger.info(MetricsNames.REBALANCE_JOB_GROUPS_FAILURE, t);
    } finally {
      rebalanceJobGroupsTimer.stop();
      if (!rebalanceJobGroupsFailure) {
        scope.counter(MetricsNames.REBALANCE_JOB_GROUPS_SUCCESS).inc(1);
        logger.info(MetricsNames.REBALANCE_JOB_GROUPS_SUCCESS);
      }
    }
  }

  @Scheduled(fixedDelayString = "${master.manager.job.rebalanceInterval}")
  public void rebalanceJobGroupsWithShadowRebalancer() throws Exception {
    if (!leaderSelector.isLeader()) {
      logger.debug(
          "skipped rebalanceJobGroupsWithShadowRebalancer because of current instance is not leader");
      return;
    }

    if (!shadowRebalancerDelegate.runShadowRebalancer()) {
      logger.debug(
          "skipped rebalanceJobGroupsWithShadowRebalancer because it's not configurable to run");
      return;
    }

    Stopwatch rebalanceJobGroupsTimer =
        scope.timer(MetricsNames.SHADOW_REBALANCE_JOB_GROUPS_LATENCY).start();
    boolean rebalanceJobGroupsFailure = false;
    try {
      // 1. Get all workers from worker store
      Map<Long, StoredWorker> workerMap = unversion(workerStore.getAll());

      // 2. Get all job groups as RebalancingJobGroup
      Map<String, Versioned<StoredJobGroup>> jobGroups = jobGroupStore.getAll();
      Map<Long, StoredJobStatus> jobStatus = unversion(jobStatusStore.getAll());

      // 3. Generate the rebalancing job groups
      Map<String, RebalancingJobGroup> rebalancingJobGroupMap =
          generateRebalancingJobGroups(jobGroups, jobStatus);

      // 4. compute load of each jobGroup / job
      shadowRebalancerDelegate.computeLoad(rebalancingJobGroupMap);

      // 5. Call computeJobConfiguration
      shadowRebalancerDelegate.computeJobConfiguration(rebalancingJobGroupMap, workerMap);

      // 6. Unset jobs assigned to stale workers.
      // We do this in the framework instead of delegating to each rebalancer impl
      // b/c this is important to ensure no zombie jobs.
      ensureValidWorkerId(rebalancingJobGroupMap.values(), workerMap.keySet());

      // 7. Call computeJobState
      // we change job state first so that canceled jobs will not be assigned to workers.
      shadowRebalancerDelegate.computeJobState(rebalancingJobGroupMap, workerMap);

      // 8. Call computeWorkerId
      shadowRebalancerDelegate.computeWorkerId(rebalancingJobGroupMap, workerMap);

      // we only need to verify the computeWorkerId method. This also means the
      // shadowRebalancerDelegate
      // should store the new assignment after every computeWorkerId is invoked

    } catch (Throwable t) {
      rebalanceJobGroupsFailure = true;
      scope.counter(MetricsNames.SHADOW_REBALANCE_JOB_GROUPS_FAILURE).inc(1);
      logger.info(MetricsNames.SHADOW_REBALANCE_JOB_GROUPS_FAILURE, t);
    } finally {
      rebalanceJobGroupsTimer.stop();
      if (!rebalanceJobGroupsFailure) {
        scope.counter(MetricsNames.SHADOW_REBALANCE_JOB_GROUPS_SUCCESS).inc(1);
        logger.info(MetricsNames.SHADOW_REBALANCE_JOB_GROUPS_SUCCESS);
      }
    }
  }

  @VisibleForTesting
  void maybeClearJobStatuses(Map<String, RebalancingJobGroup> jobGroups) {
    try {
      boolean isChanged = jobGroups.values().stream().anyMatch(RebalancingJobGroup::isChanged);
      if (isChanged) {
        Set<Long> statusIds = jobStatusStore.getAll().keySet();
        for (Long statusId : statusIds) {
          jobStatusStore.remove(statusId);
        }
      }
    } catch (Exception e) {
      logger.error("Error while removing the job statuses from store", e);
    }
  }

  private static class MetricsNames {
    static final String REBALANCE_JOB_GROUPS_SUCCESS = "manager.job.rebalance.groups.success";
    static final String REBALANCE_JOB_GROUPS_FAILURE = "manager.job.rebalance.groups.failure";
    static final String REBALANCE_JOB_GROUPS_LATENCY = "manager.job.rebalance.groups.latency";

    static final String SHADOW_REBALANCE_JOB_GROUPS_SUCCESS =
        "manager.job.shadow.rebalance.groups.success";

    static final String SHADOW_REBALANCE_JOB_GROUPS_FAILURE =
        "manager.job.shadow.rebalance.groups.failure";

    static final String SHADOW_REBALANCE_JOB_GROUPS_LATENCY =
        "manager.job.shadow.rebalance.groups.latency";
  }
}
