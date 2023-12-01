package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.data.kafka.datatransfer.controller.rebalancer.ShadowRebalancerDelegate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This is the implementation of ShadowRebalancerDelegate which is used to simulate the rebalance
 * process using the shadow rebalancer in the shadow mode.
 *
 * <pre> It will execute the following actions:
 * 1) call the shadowing rebalancer to compute new assignment
 * 2) cache the assignment in the internal data structure for next round of reabalance without affecting the actual assignment
 * </pre>
 *
 * TODO: clean up the shadow framework after the new rebalancer is rolled
 * out(https://t3.uberinternal.com/browse/KAFEP-4467).
 */
public class ShadowRebalancerDelegateImpl implements ShadowRebalancerDelegate {
  private final Rebalancer shadowRebalancer;

  private final boolean runShadow;

  // this is the map[jobGroupId, map[jobId, workerId]]
  private final Map<String, Map<Long, Long>> cachedJobGroupStatus;

  public ShadowRebalancerDelegateImpl(Rebalancer shadowRebalancer, boolean runShadow) {
    this.shadowRebalancer = shadowRebalancer;
    this.runShadow = runShadow;
    this.cachedJobGroupStatus = new HashMap<>();
  }

  @Override
  public void computeLoad(final Map<String, RebalancingJobGroup> jobGroups) {
    shadowRebalancer.computeLoad(jobGroups);
  }

  @Override
  public void computeJobConfiguration(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {
    shadowRebalancer.computeJobConfiguration(jobGroups, workers);
  }

  @Override
  public void computeJobState(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {
    shadowRebalancer.computeJobState(jobGroups, workers);
  }

  @Override
  public void computeWorkerId(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {
    consolidateJobGroupWithCacheBeforeRebalance(jobGroups, workers.keySet());
    shadowRebalancer.computeWorkerId(jobGroups, workers);
    consolidateJobGroupWithCacheAfterRebalance(jobGroups);
  }

  @Override
  public boolean runShadowRebalancer() {
    return runShadow;
  }

  private void consolidateJobGroupWithCacheBeforeRebalance(
      final Map<String, RebalancingJobGroup> jobGroups, Set<Long> existingWorkers) {
    // pre rebalance step, we need to apply the last computed and cached worker id into the input
    // jobGroups without changing other parameters
    for (Map.Entry<String, RebalancingJobGroup> jobGroupEntry : jobGroups.entrySet()) {
      String jobGroupId = jobGroupEntry.getKey();
      RebalancingJobGroup jobGroup = jobGroupEntry.getValue();
      if (!cachedJobGroupStatus.containsKey(jobGroupId)) {
        // this is a new job group, there is no-op here because the worker is already set to -1 in
        // JobManager
        continue;
      }

      Map<Long, StoredJob> allJobs = jobGroupEntry.getValue().getJobs();
      Map<Long, Long> cachedJobs = cachedJobGroupStatus.get(jobGroupId);

      for (Map.Entry<Long, StoredJob> jobEntry : allJobs.entrySet()) {
        Long jobId = jobEntry.getKey();
        if (!cachedJobs.containsKey(jobId)) {
          // this is a new job
          continue;
        }

        if (!existingWorkers.contains(cachedJobs.get(jobId))) {
          // worker no longer exist
          continue;
        }

        // update the last computed worker id to the job
        jobGroup.updateJob(
            jobId, jobEntry.getValue().toBuilder().setWorkerId(cachedJobs.get(jobId)).build());
      }
    }
  }

  private void consolidateJobGroupWithCacheAfterRebalance(
      final Map<String, RebalancingJobGroup> jobGroups) {
    // post rebalance step, update all computed assignment into cache
    cachedJobGroupStatus.clear();
    for (Map.Entry<String, RebalancingJobGroup> jobGroupEntry : jobGroups.entrySet()) {
      String jobGroupId = jobGroupEntry.getKey();
      RebalancingJobGroup jobGroup = jobGroupEntry.getValue();
      cachedJobGroupStatus.put(jobGroupId, new HashMap<>());
      for (Map.Entry<Long, StoredJob> jobEntry : jobGroup.getJobs().entrySet()) {
        cachedJobGroupStatus
            .get(jobGroupId)
            .put(jobEntry.getKey(), jobEntry.getValue().getWorkerId());
      }
    }
  }

  @VisibleForTesting
  Map<String, Map<Long, Long>> getCachedJobGroupStatus() {
    return cachedJobGroupStatus;
  }
}
