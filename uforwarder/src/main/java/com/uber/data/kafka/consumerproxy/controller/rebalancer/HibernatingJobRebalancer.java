package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * HibernatingJobRebalancer computes worker assignment for hibernating jobs
 *
 * <pre>
 * It assigns jobs in following rule:
 * - all jobs of the same job group assigned to same worker
 * - when there is enough worker, each worker assign hibernatingJobGroupPerWorker number of jobGroups
 * - when there is no enough worker, worker may get over loaded
 * - job group should be assigned to workers as even as possible
 * - job group will be unassigned if on worker available
 * </pre>
 */
public class HibernatingJobRebalancer {
  private final RebalancerConfiguration configuration;

  public HibernatingJobRebalancer(RebalancerConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Computes worker assignment for job
   *
   * @param jobGroups the job groups
   * @param workerMap the worker map from workerId to StoredWorker
   * @return the collection
   */
  Collection<Long> computeWorkerId(
      final List<RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workerMap) {

    HashBasedTable<Long, Long, RebalancingJob> table = RebalancerCommon.createTable(jobGroups);
    Set<Long> workerIds = Collections.unmodifiableSet(workerMap.keySet());

    // defensively unset jobs that are assigned to stale workers
    RebalancerCommon.ensureValidWorkerId(table, workerIds);

    // skip if no job group or no worker
    if (jobGroups.isEmpty() || workerMap.isEmpty()) {
      return Collections.EMPTY_LIST;
    }

    // merges jobs of the same jobGroup into the same worker
    Map<RebalancingJobGroup, Long> jobGroupToWorker = computeJobGroupToWorker(table);

    // computes placement plan for workers
    List<Integer> placementPlan = computePlacementPlan(jobGroupToWorker.size(), workerIds.size());

    // compute final mapping form jobGroup to worker
    jobGroupToWorker = balanceJobGroupWorker(jobGroupToWorker, workerIds, placementPlan);

    // execute plan and assign job to worker
    return assignJobToWorker(jobGroupToWorker, table);
  }

  /**
   * Executes plan to assign job to worker
   *
   * @param jobGroupToWorker map from jobGroup to target workerId
   * @param workerIds available worker ids
   * @param placementPlan number of jobGroups to place on each worker
   * @return
   */
  private Map<RebalancingJobGroup, Long> balanceJobGroupWorker(
      Map<RebalancingJobGroup, Long> jobGroupToWorker,
      Set<Long> workerIds,
      List<Integer> placementPlan) {
    Map<RebalancingJobGroup, Long> result = new HashMap<>();
    // compute worker job groups
    Map<Long, List<RebalancingJobGroup>> workerJobGroupMap =
        jobGroupToWorker
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getValue(),
                    entry -> Arrays.asList(entry.getKey()),
                    (list1, list2) ->
                        Stream.concat(list1.stream(), list2.stream())
                            .collect(Collectors.toList())));
    workerIds
        .stream()
        .forEach(
            workerId -> workerJobGroupMap.computeIfAbsent(workerId, o -> Collections.EMPTY_LIST));

    // non-empty workers sorted by number of jobGroups in ascending order
    List<Map.Entry<Long, List<RebalancingJobGroup>>> workerJobGroupList =
        workerJobGroupMap
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey() != WorkerUtils.UNSET_WORKER_ID)
            .sorted(Comparator.comparingInt(entry -> entry.getValue().size()))
            .collect(Collectors.toList());

    int nSpareWorkers = workerJobGroupList.size() - placementPlan.size();
    // free spare workers
    List<RebalancingJobGroup> freeJobGroups =
        workerJobGroupList
            .subList(0, nSpareWorkers)
            .stream()
            .map(entry -> entry.getValue())
            .flatMap(list -> list.stream())
            .collect(Collectors.toList());
    // include all unassigned job groups
    freeJobGroups.addAll(
        workerJobGroupMap.getOrDefault(WorkerUtils.UNSET_WORKER_ID, Collections.EMPTY_LIST));

    workerJobGroupList = workerJobGroupList.subList(nSpareWorkers, workerJobGroupList.size());

    // switch to mutable list
    workerJobGroupList.stream().forEach(entry -> entry.setValue(new ArrayList<>(entry.getValue())));

    // make sure worker under load
    for (int i = 0; i < placementPlan.size(); ++i) {
      Map.Entry<Long, List<RebalancingJobGroup>> entry = workerJobGroupList.get(i);
      int expectedSize = placementPlan.get(i);
      int actualSize = entry.getValue().size();
      if (actualSize > expectedSize) {
        List<RebalancingJobGroup> jobGroups = entry.getValue().subList(0, expectedSize);
        freeJobGroups.addAll(entry.getValue().subList(expectedSize, actualSize));
        entry.setValue(jobGroups);
      }
    }

    // assign free work load to worker
    LinkedList<RebalancingJobGroup> linkFreeJobGroups = new LinkedList<>(freeJobGroups);
    for (int i = 0; i < placementPlan.size(); ++i) {
      Map.Entry<Long, List<RebalancingJobGroup>> entry = workerJobGroupList.get(i);
      int expectedSize = placementPlan.get(i);
      int actualSize = entry.getValue().size();
      for (int j = 0; j < expectedSize - actualSize; ++j) {
        entry.getValue().add(linkFreeJobGroups.poll());
      }
    }

    // collect result
    workerJobGroupList
        .stream()
        .forEach(
            entry -> entry.getValue().stream().forEach(group -> result.put(group, entry.getKey())));

    return result;
  }

  /**
   * Assigns job to workers according to the plan
   *
   * @param jobGroupToWorker job group to worker mapping plan
   * @param table jobId-workerId-Job mapping
   * @return collection of used workerIds
   */
  private Collection<Long> assignJobToWorker(
      Map<RebalancingJobGroup, Long> jobGroupToWorker,
      HashBasedTable<Long, Long, RebalancingJob> table) {
    Set<Long> result = new HashSet<>();
    for (RebalancingJob job : table.values()) {
      RebalancingJobGroup jobGroup = job.getJobGroup();
      long workerId = jobGroupToWorker.getOrDefault(jobGroup, WorkerUtils.UNSET_WORKER_ID);
      result.add(workerId);
      if (job.getWorkerId() != workerId) {
        job.setWorkerId(workerId);
      }
    }
    result.remove(WorkerUtils.UNSET_WORKER_ID);
    return result;
  }

  /**
   * Computes existing mapping from job group to worker if one job group has been assigned to
   * multiple workers, the result will only contains one
   *
   * @param table jobId-workerId-Job mapping
   * @return existing mapping from job group to worker
   */
  private Map<RebalancingJobGroup, Long> computeJobGroupToWorker(
      HashBasedTable<Long, Long, RebalancingJob> table) {
    Map<RebalancingJobGroup, Long> jobGroupToWorker = new HashMap<>();
    for (RebalancingJob job : table.values()) {
      RebalancingJobGroup jobGroup = job.getJobGroup();
      jobGroupToWorker.computeIfAbsent(jobGroup, o -> job.getWorkerId());
    }

    return jobGroupToWorker;
  }

  /**
   * Computes number of job groups to place on each worker
   *
   * @param nJobGroups number of job groups
   * @param nWorkers number of workers
   * @return
   */
  List<Integer> computePlacementPlan(int nJobGroups, int nWorkers) {
    int maxJobGroupsPerWorker = configuration.getHibernatingJobGroupPerWorker();
    int workerToUse =
        Math.min(nWorkers, (int) Math.ceil((double) nJobGroups / maxJobGroupsPerWorker));
    if (workerToUse == 0) {
      return Collections.EMPTY_LIST;
    }
    int nHighWorkers = nJobGroups % workerToUse;
    int nLowWorkers = workerToUse - nJobGroups % workerToUse;
    int lowJobGroupsPerWorker = nJobGroups / workerToUse;
    ImmutableList.Builder<Integer> result = ImmutableList.builder();
    result.addAll(Collections.nCopies(nLowWorkers, lowJobGroupsPerWorker));
    result.addAll(Collections.nCopies(nHighWorkers, lowJobGroupsPerWorker + 1));
    return result.build();
  }
}
