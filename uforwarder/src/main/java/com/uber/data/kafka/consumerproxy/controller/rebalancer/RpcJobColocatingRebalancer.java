package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import static com.uber.data.kafka.consumerproxy.controller.rebalancer.RebalancerCommon.TARGET_UNIT_NUMBER;
import static com.uber.data.kafka.consumerproxy.controller.rebalancer.RebalancerCommon.WORKLOAD_CAPACITY_PER_WORKER;
import static com.uber.data.kafka.consumerproxy.controller.rebalancer.RebalancerCommon.roundUpToNearestNumber;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.m3.tally.Scope;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a Rebalancer implementation which can colocate the different RpcJobs with different URI
 * on same workers. It's built on top of consistent-hashing. Please refer to
 * https://docs.google.com/document/d/1hhPbfVKMbk_ZBZkT0I0Ciz2eYi95MR_pJK6WzroLm1s/edit for design.
 *
 * <pre>The rebalance will have multiple steps:
 *
 *  1) calculate the hash values of each worker and assign them into corresponding partition/range of the ring
 *
 *  2) calculate the corresponding partition for each job group and mark those job groups that are on the wrong workers
 *
 *  3) place the stale job groups from 2) to the correct workers
 *
 *  4) adjust the workload on each worker, move jobs to another worker that can hold the jobs Note: 3) and 4) only move jobs between workers in the same partition
 *
 *  </pre>
 */
public class RpcJobColocatingRebalancer extends AbstractRpcUriRebalancer {
  private static final Logger logger = LoggerFactory.getLogger(RpcJobColocatingRebalancer.class);

  private static final String VIRTUAL_PARTITION_TAG = "virtual_partition";

  private static final String COLOCATING_REBALANCER_SUB_SCOPE = "colocating_rebalancer";

  protected final RebalancerConfiguration rebalancerConfiguration;

  private final Scope scope;

  private final boolean shadowRun;
  private final RebalancingWorkerTable rebalancingWorkerTable;

  public RpcJobColocatingRebalancer(
      Scope scope,
      RebalancerConfiguration rebalancerConfiguration,
      Scalar scalar,
      HibernatingJobRebalancer hibernatingJobRebalancer,
      boolean shadowRun) {
    super(scope, rebalancerConfiguration, scalar, hibernatingJobRebalancer);
    this.rebalancerConfiguration = rebalancerConfiguration;
    this.scope = scope;
    this.shadowRun = shadowRun;
    this.rebalancingWorkerTable = new RebalancingWorkerTable();
  }

  @Override
  public void computeWorkerId(
      final Map<String, RebalancingJobGroup> jobGroupMap, final Map<Long, StoredWorker> workerMap) {
    // reset all in memory job and worker data
    rebalancingWorkerTable.clear();

    Map<String, Integer> jobGroupToPartitionMap = new HashMap<>();
    List<Integer> workerNeededPerPartition =
        RebalancerCommon.generateWorkerVirtualPartitions(
            jobGroupMap,
            workerMap,
            rebalancerConfiguration,
            jobGroupToPartitionMap,
            rebalancingWorkerTable);

    // step 1: move job/jobGroup to correct virtual partitions first
    List<StaleWorkerReplacement> toBeMovedStaleJobs = new ArrayList<>();
    assignJobsToCorrectVirtualPartition(jobGroupMap, toBeMovedStaleJobs, jobGroupToPartitionMap);

    // step 2: handle jobs on stale workers after workload adjustment
    handleJobsOnStaleWorkers(toBeMovedStaleJobs);

    // step 3: adjust the workload of each worker one by one
    ensureWorkersLoadBalanced();

    // step 4: convert to the final result
    for (RebalancingWorkerWithSortedJobs worker : rebalancingWorkerTable.getAllWorkers()) {
      List<RebalancingJob> jobs = worker.getAllJobs();
      jobs.forEach(job -> job.setWorkerId(worker.getWorkerId()));
    }

    // step 5: emit metrics
    emitMetrics(workerNeededPerPartition);
  }

  protected RebalancingWorkerTable getRebalancingTable() {
    return rebalancingWorkerTable;
  }

  private void assignJobsToCorrectVirtualPartition(
      Map<String, RebalancingJobGroup> jobGroupMap,
      List<StaleWorkerReplacement> toBeMovedStaleJobs,
      Map<String, Integer> jobGroupToPartitionMap) {
    for (RebalancingJobGroup jobGroup : jobGroupMap.values()) {
      String jobGroupId = jobGroup.getJobGroup().getJobGroupId();
      Preconditions.checkArgument(jobGroupToPartitionMap.containsKey(jobGroupId));
      long partitionIdx = jobGroupToPartitionMap.get(jobGroupId);

      List<StoredJob> allJobs = new ArrayList<>(jobGroup.getJobs().values());
      StaleWorkerReplacement staleWorkerReplacement =
          new StaleWorkerReplacement(jobGroup, partitionIdx);
      for (StoredJob job : allJobs) {
        if (job.getState() != JobState.JOB_STATE_RUNNING) {
          continue;
        }

        long currentWorkerId = job.getWorkerId();
        // job is not on the correct worker
        if (!rebalancingWorkerTable.isWorkerIdValid(currentWorkerId)
            || !rebalancingWorkerTable
                .getAllWorkerIdsForPartition(partitionIdx)
                .contains(currentWorkerId)) {
          staleWorkerReplacement.addStoredJob(job);
        } else {
          rebalancingWorkerTable
              .getRebalancingWorkerWithSortedJobs(currentWorkerId)
              .addJob(new RebalancingJob(job, jobGroup));
        }
      }
      if (staleWorkerReplacement.storedJobs.size() != 0) {
        toBeMovedStaleJobs.add(staleWorkerReplacement);
      }
    }
  }

  private void handleJobsOnStaleWorkers(List<StaleWorkerReplacement> toBeMovedStaleJobs) {
    int totalStaleJobs = 0;
    for (StaleWorkerReplacement replacement : toBeMovedStaleJobs) {
      totalStaleJobs += replacement.storedJobs.size();
      // always start from the least loaded worker
      PriorityQueue<RebalancingWorkerWithSortedJobs> candidateWorkers = new PriorityQueue<>();
      candidateWorkers.addAll(
          rebalancingWorkerTable.getAllWorkersForPartition(replacement.virtualPartitionIndex));
      // TODO emit metric if candidate worker for a partition is empty
      // TODO: can consider move these jobs to other workers for short mitigation
      if (candidateWorkers.isEmpty()) {
        // The probability of this is low
        logger.warn(
            "There is no workers for the partition {} of job group {}.",
            replacement.virtualPartitionIndex,
            replacement.jobGroup);
        continue;
      }

      replacement.storedJobs.forEach(
          job -> {
            // there should be always a worker
            Preconditions.checkArgument(!candidateWorkers.isEmpty());
            RebalancingWorkerWithSortedJobs leastLoadedWorker = candidateWorkers.poll();
            leastLoadedWorker.addJob(new RebalancingJob(job, replacement.jobGroup));
            candidateWorkers.add(leastLoadedWorker);
          });
    }

    scope
        .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
        .gauge(MetricNames.STALE_JOB_COUNT)
        .update(totalStaleJobs);
  }

  private void ensureWorkersLoadBalanced() {
    for (long partitionIdx : rebalancingWorkerTable.getAllPartitions()) {
      List<RebalancingWorkerWithSortedJobs> allWorkersInPartition =
          rebalancingWorkerTable.getAllWorkersForPartition(partitionIdx);
      // TODO: emit metric if the workers within partition is empty
      List<RebalancingWorkerWithSortedJobs> allWorkers = new ArrayList<>(allWorkersInPartition);
      allWorkers.sort(RebalancingWorkerWithSortedJobs::compareTo);

      int numberOfOverloadingWorker = 0;
      int numberOfUnadjustedWorker = 0;
      // starting from the most loaded worker
      for (int workerIdx = allWorkers.size() - 1; workerIdx >= 0; workerIdx--) {
        RebalancingWorkerWithSortedJobs rebalancingWorkerWithSortedJobs = allWorkers.get(workerIdx);
        if (rebalancingWorkerWithSortedJobs.getNumberOfJobs() == 1) {
          // we don't move if the worker only has one job
          continue;
        }
        if (!isWorkerUnderLoadLimit(rebalancingWorkerWithSortedJobs)) {
          numberOfOverloadingWorker += 1;
          boolean adjustedLoad =
              adjustJobsOnWorker(rebalancingWorkerWithSortedJobs, workerIdx, allWorkers);
          if (!adjustedLoad) {
            numberOfUnadjustedWorker += 1;
            logger.warn(
                "Worker is overloaded after adjusting workload.",
                StructuredLogging.workerId(rebalancingWorkerWithSortedJobs.getWorkerId()),
                StructuredLogging.count(rebalancingWorkerWithSortedJobs.getNumberOfJobs()),
                StructuredLogging.workloadScale(rebalancingWorkerWithSortedJobs.getLoad()),
                StructuredLogging.virtualPartition(partitionIdx));
          }
        }
      }

      scope
          .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
          .tagged(ImmutableMap.of(VIRTUAL_PARTITION_TAG, Long.toString(partitionIdx)))
          .gauge(MetricNames.OVERLOAD_WORKER_NUMBER)
          .update(numberOfOverloadingWorker);
      scope
          .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
          .tagged(ImmutableMap.of(VIRTUAL_PARTITION_TAG, Long.toString(partitionIdx)))
          .gauge(MetricNames.UNADJUSTED_WORKLOAD_WORKER)
          .update(numberOfUnadjustedWorker);

      // do a second traverse to make sure we don't have overloaded job(>1.0) on same worker
      for (int workerIdx = allWorkers.size() - 1; workerIdx >= 0; workerIdx--) {
        RebalancingWorkerWithSortedJobs rebalancingWorkerWithSortedJobs = allWorkers.get(workerIdx);
        if (rebalancingWorkerWithSortedJobs.getNumberOfJobs() == 1) {
          // we don't move if the worker only has one job
          continue;
        }

        if (!isWorkerUnderLoadLimit(rebalancingWorkerWithSortedJobs)) {
          boolean adjustedLoad =
              ensureOverloadedJobsMovedToIdleWorker(
                  rebalancingWorkerWithSortedJobs, workerIdx, allWorkers);
          if (!adjustedLoad) {
            logger.warn(
                "Worker is still overloaded after adjusting large workload.",
                StructuredLogging.workerId(rebalancingWorkerWithSortedJobs.getWorkerId()),
                StructuredLogging.count(rebalancingWorkerWithSortedJobs.getNumberOfJobs()),
                StructuredLogging.workloadScale(rebalancingWorkerWithSortedJobs.getLoad()),
                StructuredLogging.virtualPartition(partitionIdx));
          }
        }
      }
    }
  }

  private boolean ensureOverloadedJobsMovedToIdleWorker(
      RebalancingWorkerWithSortedJobs adjustedWorker,
      int toAdjustWorkerIdx,
      List<RebalancingWorkerWithSortedJobs> allWorkersInPartition) {
    List<RebalancingJob> allJobsInQueue = new ArrayList<>(adjustedWorker.getAllJobs());
    allJobsInQueue.sort(RebalancingJob::compareTo);
    boolean adjustedLoad = false;
    // starting from the most loaded job
    for (RebalancingJob toBeMovedJob : allJobsInQueue) {
      if (toBeMovedJob.getLoad() <= 1.0) {
        // if the job is with load <= 1.0, which means it should considered in #adjustJobsOnWorker
        // and we can cancel the adjust
        break;
      }

      long newWorkerId = -1L;
      for (int otherWorkerIdx = 0; otherWorkerIdx < toAdjustWorkerIdx; otherWorkerIdx++) {
        RebalancingWorkerWithSortedJobs otherWorker = allWorkersInPartition.get(otherWorkerIdx);
        if (otherWorker.getNumberOfJobs() == 0) {
          newWorkerId = otherWorker.getWorkerId();
          break;
        }
      }

      if (newWorkerId != -1L) {
        adjustedWorker.removeJob(toBeMovedJob);
        rebalancingWorkerTable.getRebalancingWorkerWithSortedJobs(newWorkerId).addJob(toBeMovedJob);
        scope.subScope(COLOCATING_REBALANCER_SUB_SCOPE).counter(MetricNames.JOB_MOVEMENT).inc(1);
      }

      if (adjustedWorker.getNumberOfJobs() == 1) {
        adjustedLoad = true;
        break;
      }
    }

    return adjustedLoad;
  }

  private boolean adjustJobsOnWorker(
      RebalancingWorkerWithSortedJobs adjustedWorker,
      int toAdjustWorkerIdx,
      List<RebalancingWorkerWithSortedJobs> allWorkersInPartition) {
    List<RebalancingJob> allJobsInQueue = new ArrayList<>(adjustedWorker.getAllJobs());
    allJobsInQueue.sort(RebalancingJob::compareTo);

    boolean adjustedLoad = false;
    // starting from the most loaded job
    for (RebalancingJob toBeMovedJob : allJobsInQueue) {
      long newWorkerId = -1L;
      for (int otherWorkerIdx = 0; otherWorkerIdx < toAdjustWorkerIdx; otherWorkerIdx++) {
        RebalancingWorkerWithSortedJobs otherWorker = allWorkersInPartition.get(otherWorkerIdx);
        if (otherWorker.getLoad() + toBeMovedJob.getLoad() <= WORKLOAD_CAPACITY_PER_WORKER
            && otherWorker.getNumberOfJobs() + 1
                <= rebalancerConfiguration.getMaxJobNumberPerWorker()) {
          newWorkerId = otherWorker.getWorkerId();
          break;
        }
      }
      // if we can't find the worker, it means:
      // 1. there is no worker that can hold the new workload if all workers already have more
      // workload than expected
      // 2. there is no worker that can hold the new workload if all workers already have more jobs
      // than expected
      // in either case, the rest of the workers will exceed the capacity
      // and we shouldn't move the jobs to overload other workers.
      if (newWorkerId != -1L) {
        adjustedWorker.removeJob(toBeMovedJob);
        rebalancingWorkerTable.getRebalancingWorkerWithSortedJobs(newWorkerId).addJob(toBeMovedJob);
        scope.subScope(COLOCATING_REBALANCER_SUB_SCOPE).counter(MetricNames.JOB_MOVEMENT).inc(1);
      }

      if (isWorkerUnderLoadLimit(adjustedWorker)) {
        adjustedLoad = true;
        break;
      }
    }
    return adjustedLoad;
  }

  private boolean isWorkerUnderLoadLimit(RebalancingWorkerWithSortedJobs worker) {
    return worker.getLoad() <= WORKLOAD_CAPACITY_PER_WORKER
        && worker.getNumberOfJobs() <= rebalancerConfiguration.getMaxJobNumberPerWorker();
  }

  private void emitMetrics(List<Integer> workerNeededPerPartition) {
    int usedWorkers = 0;
    for (RebalancingWorkerWithSortedJobs worker : rebalancingWorkerTable.getAllWorkers()) {
      if (worker.getNumberOfJobs() != 0) {
        usedWorkers += 1;
      }
    }
    scope.gauge(MetricNames.USED_WORKER_COUNT).update(usedWorkers);

    int totalNumberOfWorkersStillNeeded = 0;
    for (long partitionIdx : rebalancingWorkerTable.getAllPartitions()) {
      usedWorkers = 0;
      Map<String, String> partitionTags =
          ImmutableMap.of(VIRTUAL_PARTITION_TAG, Long.toString(partitionIdx));
      DoubleSummaryStatistics stats = new DoubleSummaryStatistics();
      List<RebalancingWorkerWithSortedJobs> allWorkersWithinPartition =
          rebalancingWorkerTable.getAllWorkersForPartition(partitionIdx);
      scope
          .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
          .tagged(partitionTags)
          .gauge(MetricNames.ASSIGNED_WORKER_NUMBER_IN_PARTITION)
          .update(allWorkersWithinPartition.size());

      scope
          .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
          .tagged(partitionTags)
          .gauge(MetricNames.REQUESTED_WORKER_NUMBER_IN_PARTITION)
          .update(workerNeededPerPartition.get((int) partitionIdx));

      totalNumberOfWorkersStillNeeded +=
          workerNeededPerPartition.get((int) partitionIdx) - allWorkersWithinPartition.size();

      for (RebalancingWorkerWithSortedJobs worker : allWorkersWithinPartition) {
        if (worker.getNumberOfJobs() > 0) {
          usedWorkers += 1;
        }
        Map<String, String> workerTags =
            ImmutableMap.of(
                VIRTUAL_PARTITION_TAG,
                Long.toString(partitionIdx),
                MetricNames.WORKER_IDX,
                Long.toString(worker.getWorkerId()));
        stats.accept(worker.getLoad());
        scope
            .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
            .tagged(workerTags)
            .gauge(MetricNames.WORKER_EXPECTED_LOAD)
            .update(worker.getLoad());
      }

      scope
          .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
          .tagged(partitionTags)
          .gauge(MetricNames.WORKER_LOAD_AVG)
          .update(stats.getAverage());
      scope
          .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
          .tagged(partitionTags)
          .gauge(MetricNames.WORKER_LOAD_MAX)
          .update(stats.getMax());
      scope
          .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
          .tagged(partitionTags)
          .gauge(MetricNames.WORKER_LOAD_MIN)
          .update(stats.getMin());

      double standardDeviation = 0.0;
      for (RebalancingWorkerWithSortedJobs worker :
          rebalancingWorkerTable.getAllWorkersForPartition(partitionIdx)) {
        if (worker.getNumberOfJobs() == 0) {
          continue;
        }

        standardDeviation += Math.pow(worker.getLoad() - stats.getAverage(), 2);
      }

      scope
          .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
          .tagged(partitionTags)
          .gauge(MetricNames.WORKER_LOAD_STD_DEVIATION)
          .update(standardDeviation / usedWorkers);
    }

    scope
        .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
        .gauge(MetricNames.EXTRA_WORKERS_UNFULFILLED)
        .update(totalNumberOfWorkersStillNeeded);

    int totalRequestedNumberOfWorker =
        workerNeededPerPartition.stream().mapToInt(Integer::intValue).sum();
    totalRequestedNumberOfWorker =
        roundUpToNearestNumber(totalRequestedNumberOfWorker, TARGET_UNIT_NUMBER);

    if (shadowRun) {
      // if it's shadow, still emit metric for comparison
      scope.gauge(MetricNames.REQUESTED_WORKER_COUNT).update(totalRequestedNumberOfWorker);
    } else {
      scope.gauge(MetricNames.WORKERS_TARGET).update(totalRequestedNumberOfWorker);
    }
  }

  private static class StaleWorkerReplacement {
    private final RebalancingJobGroup jobGroup;
    private final List<StoredJob> storedJobs;
    private final long virtualPartitionIndex;

    StaleWorkerReplacement(RebalancingJobGroup jobGroup, long virtualPartitionIndex) {
      this.jobGroup = jobGroup;
      this.storedJobs = new ArrayList<>();
      this.virtualPartitionIndex = virtualPartitionIndex;
    }

    void addStoredJob(StoredJob storedJob) {
      this.storedJobs.add(storedJob);
    }
  }

  /**
   * RebalancingWorkerTable is an internal data structure to keep track of each worker and the
   * corresponding virtual node, as well as the jobs on each worker
   */
  @VisibleForTesting
  protected static class RebalancingWorkerTable {
    private final Map<Long, Long> workerIdToVirtualPartitionMap = new HashMap<>();
    private final Map<Long, Set<Long>> virtualPartitionToWorkerIdMap = new HashMap<>();
    private final Map<Long, RebalancingWorkerWithSortedJobs> workerIdToWorkerMap = new HashMap<>();

    RebalancingWorkerTable() {}

    boolean isWorkerIdValid(long workerId) {
      return workerIdToWorkerMap.containsKey(workerId);
    }

    void put(long workerId, long rangeIndex) {
      workerIdToVirtualPartitionMap.put(workerId, rangeIndex);
      virtualPartitionToWorkerIdMap.putIfAbsent(rangeIndex, new HashSet<>());
      virtualPartitionToWorkerIdMap.get(rangeIndex).add(workerId);
      workerIdToWorkerMap.put(
          workerId, new RebalancingWorkerWithSortedJobs(workerId, 0, ImmutableList.of()));
    }

    boolean putIfAbsent(long workerId, long rangeIndex) {
      Long workerVirtualPartition = workerIdToVirtualPartitionMap.get(workerId);
      if (workerVirtualPartition == null) {
        put(workerId, rangeIndex);
        return true;
      } else if (workerVirtualPartition != rangeIndex) {
        logger.warn(
            "Worker is already assigned to a different virtual partition, skipping.",
            StructuredLogging.workerId(workerId),
            StructuredLogging.virtualPartition(workerVirtualPartition),
            StructuredLogging.skippedVirtualPartition(rangeIndex));
      }
      return false;
    }

    List<RebalancingWorkerWithSortedJobs> getAllWorkersForPartition(long partitionIdx) {
      if (!virtualPartitionToWorkerIdMap.containsKey(partitionIdx)) {
        return ImmutableList.of();
      }

      return virtualPartitionToWorkerIdMap
          .get(partitionIdx)
          .stream()
          .map(workerIdToWorkerMap::get)
          .collect(Collectors.toList());
    }

    Set<Long> getAllWorkerIdsForPartition(long partitionIdx) {
      return getAllWorkersForPartition(partitionIdx)
          .stream()
          .map(RebalancingWorkerWithSortedJobs::getWorkerId)
          .collect(Collectors.toSet());
    }

    RebalancingWorkerWithSortedJobs getRebalancingWorkerWithSortedJobs(long workerId) {
      Preconditions.checkArgument(workerIdToWorkerMap.containsKey(workerId));
      return workerIdToWorkerMap.get(workerId);
    }

    List<RebalancingWorkerWithSortedJobs> getAllWorkers() {
      return ImmutableList.copyOf(workerIdToWorkerMap.values());
    }

    Set<Long> getAllWorkerIds() {
      return ImmutableSet.copyOf(workerIdToWorkerMap.keySet());
    }

    List<Long> getAllPartitions() {
      return ImmutableList.copyOf(virtualPartitionToWorkerIdMap.keySet());
    }

    void removeWorker(long workerId) {
      if (workerIdToVirtualPartitionMap.containsKey(workerId)) {
        long partitionId = workerIdToVirtualPartitionMap.remove(workerId);
        Preconditions.checkArgument(virtualPartitionToWorkerIdMap.containsKey(partitionId));
        virtualPartitionToWorkerIdMap.get(partitionId).remove(workerId);
        workerIdToWorkerMap.remove(workerId);
      }
    }

    void clear() {
      workerIdToVirtualPartitionMap.clear();
      virtualPartitionToWorkerIdMap.clear();
      workerIdToWorkerMap.clear();
    }
  }

  static class MetricNames {
    static final String WORKERS_TARGET = "rebalancer.workers.target";

    private static final String REQUESTED_WORKER_COUNT = "colocatingrebalancer.requested.worker";

    private static final String USED_WORKER_COUNT = "colocatingrebalancer.used.worker";

    private static final String WORKER_LOAD_AVG = "worker.load.avg";

    private static final String WORKER_LOAD_STD_DEVIATION = "worker.load.sd";

    private static final String WORKER_LOAD_MAX = "worker.load.max";

    private static final String WORKER_LOAD_MIN = "worker.load.min";

    private static final String WORKER_EXPECTED_LOAD = "worker.load.expected";

    private static final String WORKER_IDX = "worker.idx";

    private static final String OVERLOAD_WORKER_NUMBER = "overload.worker.number";

    private static final String UNADJUSTED_WORKLOAD_WORKER = "unadjusted.worker.number";

    private static final String ASSIGNED_WORKER_NUMBER_IN_PARTITION =
        "per.partition.assigned.worker.number";

    private static final String REQUESTED_WORKER_NUMBER_IN_PARTITION =
        "per.partition.requested.worker.number";

    private static final String EXTRA_WORKERS_UNFULFILLED = "unfulfilled.extra.workers";

    private static final String JOB_MOVEMENT = "job.movement";

    private static final String STALE_JOB_COUNT = "stale.job.count";

    private MetricNames() {}
  }
}
