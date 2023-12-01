package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.data.kafka.datatransfer.StoredWorker;
import java.util.Map;

/**
 * ShadowRebalancerDelegate is an interface that allows plugging in different rebalancers for
 * shadowing
 */
public interface ShadowRebalancerDelegate {

  /**
   * Before assign job to worker, compute load of reach jobGroup
   *
   * @param jobGroups the job groups
   */
  default void computeLoad(final Map<String, RebalancingJobGroup> jobGroups) {}

  /**
   * Compute new job configuration by updating the mutable {@link RebalancingJobGroup}.
   *
   * @param jobGroups that are registered to this data-transfer cluster.
   * @param workers is the set of live workers
   */
  default void computeJobConfiguration(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {}

  /**
   * Compute new job state by updating the mutable {@link RebalancingJobGroup}.
   *
   * @param jobGroups that are registered to this data-transfer cluster.
   * @param workers is the set of live workers
   */
  default void computeJobState(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {}

  /**
   * Compute new worker_id by updating the mutable {@link RebalancingJobGroup}.
   *
   * @param jobGroups that are registered to this data-transfer cluster.
   * @param workers is the set of live workers
   */
  default void computeWorkerId(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {}

  /**
   * Return shadow mode status
   *
   * @return False as default
   */
  default boolean runShadowRebalancer() {
    return false;
  }
}
