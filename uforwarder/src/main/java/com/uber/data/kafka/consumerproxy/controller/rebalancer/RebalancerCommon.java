package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Rebalancer utility functions */
class RebalancerCommon {

  // this is the unit number to round up when calculating the expecetd worker number for the cluster
  static final int TARGET_UNIT_NUMBER = 5;

  static final double WORKLOAD_CAPACITY_PER_WORKER = 1.0;

  private static final int MINIMUM_WORKER_THRESHOLD = 2;

  private static final Logger logger = LoggerFactory.getLogger(RebalancerCommon.class);

  /**
   * Creates a Guava {@code Table<Long, Long, RebalancingJob>} contains the assignment.
   *
   * @param jobGroups is the rebalancing job groups passed in from the data transfer framework
   * @return a TreeBasedTable that stores the assignment state.
   *     <p>Note: the table filters for jobs in JOB_STATE_RUNNING so that only RUNNING jobs are
   *     considered in the rebalance computation.
   */
  static HashBasedTable<Long, Long, RebalancingJob> createTable(
          List<RebalancingJobGroup> jobGroups) {
    // A guava table that contains (job_id, worker_id, messages_per_sec) which contain the
    // assignment
    HashBasedTable<Long, Long, RebalancingJob> table = HashBasedTable.create();

    // load jobs into the table
    for (RebalancingJobGroup jobGroup : jobGroups) {
      for (StoredJob job : jobGroup.getJobs().values()) {
        if (job.getState() == JobState.JOB_STATE_RUNNING) {
          table.put(job.getJob().getJobId(), job.getWorkerId(), new RebalancingJob(job, jobGroup));
        }
      }
    }

    return table;
  }

  /**
   * Ensures that the jobs in the table are assigned to valid workers from the workerIds set.
   *
   * <p>Jobs that were assigned to stale workers have their assignment modified to UNSET_WORKER_ID
   * in table.
   */
  static void ensureValidWorkerId(
          HashBasedTable<Long, Long, RebalancingJob> table, Set<Long> workerIds) {
    Set<Long> jobIds = ImmutableSet.copyOf(table.rowKeySet());
    jobIds.forEach(
            jobId -> {
              Map<Long, RebalancingJob> assignment = table.row(jobId);
              Preconditions.checkState(assignment.size() == 1, "expect 1 assignment for a jobId");
              long workerId = assignment.keySet().iterator().next();
              if (workerId != WorkerUtils.UNSET_WORKER_ID && !workerIds.contains(workerId)) {
                RebalancingJob job = assignment.get(workerId);
                Preconditions.checkNotNull(
                        job, "job should exist because Guava table enforces tuple existance");
                table.remove(jobId, workerId);
                table.put(jobId, WorkerUtils.UNSET_WORKER_ID, job);
                job.setWorkerId(WorkerUtils.UNSET_WORKER_ID);
              }
            });
  }

  static List<Integer> generateWorkerVirtualPartitions(
          final Map<String, RebalancingJobGroup> jobGroupMap,
          final Map<Long, StoredWorker> workerMap,
          RebalancerConfiguration rebalancerConfiguration,
          Map<String, Integer> jobGroupToPartitionMap,
          RpcJobColocatingRebalancer.RebalancingCache rebalancingCache) {
    int numberOfPartition = rebalancerConfiguration.getNumberOfVirtualPartitions();
    // calculate how many workers are needed per partition based on workload
    List<Integer> workerNeededPerPartition =
            calculateWorkerNeededPerPartition(
                    rebalancerConfiguration, jobGroupToPartitionMap, jobGroupMap, workerMap);
    insertWorkersIntoRebalancingTable(
            rebalancingCache,
            workerNeededPerPartition,
            workerMap,
            numberOfPartition,
            rebalancerConfiguration,
            jobGroupToPartitionMap,
            jobGroupMap);
    return workerNeededPerPartition;
  }

  private static List<Integer> calculateWorkerNeededPerPartition(
          RebalancerConfiguration rebalancerConfiguration,
          final Map<String, Integer> jobGroupToPartitionMap,
          final Map<String, RebalancingJobGroup> jobGroupMap,
          final Map<Long, StoredWorker> workerMap) {
    int numberOfPartition = rebalancerConfiguration.getNumberOfVirtualPartitions();
    // calculate the total workload of job group per partition
    List<Integer> overloadedWorkerNeededByPartitionList =
            new ArrayList<>(Collections.nCopies(numberOfPartition, 0));
    List<List<Double>> workloadPerJobByPartitionList = new ArrayList<>();
    for (int idx = 0; idx < numberOfPartition; idx++) {
      workloadPerJobByPartitionList.add(new ArrayList<>());
    }

    List<Integer> jobCountByPartitionList =
            new ArrayList<>(Collections.nCopies(numberOfPartition, 0));
    for (RebalancingJobGroup jobGroup : jobGroupMap.values()) {
      long hashValue =
              Math.abs(
                      jobGroup.getJobGroup().getJobGroupId().hashCode()
                              % rebalancerConfiguration.getMaxAssignmentHashValueRange());
      int partitionIdx = (int) (hashValue % numberOfPartition);
      jobGroupToPartitionMap.put(jobGroup.getJobGroup().getJobGroupId(), partitionIdx);
      for (StoredJob job : jobGroup.getJobs().values()) {
        // for single job with >= 1.0 scale, we need to put in a dedicated worker
        if (job.getScale() >= 1.0) {
          overloadedWorkerNeededByPartitionList.set(
                  partitionIdx, overloadedWorkerNeededByPartitionList.get(partitionIdx) + 1);
        } else {
          jobCountByPartitionList.set(partitionIdx, jobCountByPartitionList.get(partitionIdx) + 1);
          workloadPerJobByPartitionList.get(partitionIdx).add(job.getScale());
        }
      }
    }

    // calculate how many workers are needed per partition based on workload
    List<Integer> workersNeededForPartition = new ArrayList<>();
    for (int idx = 0; idx < numberOfPartition; idx++) {
      int expectedNumberOfWorkerForWorkload =
              getWorkerNumberPerWorkload(workloadPerJobByPartitionList.get(idx));
      int expectedNumberOfWorkerForJobCount =
              (jobCountByPartitionList.get(idx)
                      + rebalancerConfiguration.getMaxJobNumberPerWorker()
                      - 1)
                      / rebalancerConfiguration.getMaxJobNumberPerWorker();
      int neededNumberOfWorkerWithoutOverloadWorker =
              Math.max(expectedNumberOfWorkerForWorkload, expectedNumberOfWorkerForJobCount);
      // leave some spare to handle traffic increase
      int neededNumberOfWorker =
              (int)
                      Math.ceil(
                              neededNumberOfWorkerWithoutOverloadWorker
                                      * (1
                                      + (double) rebalancerConfiguration.getTargetSpareWorkerPercentage()
                                      / 100))
                      + overloadedWorkerNeededByPartitionList.get(idx);

      workersNeededForPartition.add(neededNumberOfWorker);
    }

    return workersNeededForPartition;
  }

  private static int getWorkerNumberPerWorkload(List<Double> workloadPerJob) {
    // greedy approach to calculate number of workers needed, sorting from high to low
    workloadPerJob.sort(Comparator.reverseOrder());
    int numberOfWorker = 0;
    double currentWorkload = 0.0;
    for (double workload : workloadPerJob) {
      if (currentWorkload + workload < WORKLOAD_CAPACITY_PER_WORKER) {
        currentWorkload += workload;
      } else {
        currentWorkload = workload;
        numberOfWorker += 1;
      }
    }

    if (currentWorkload > 0) {
      numberOfWorker += 1;
    }

    return numberOfWorker;
  }

  @VisibleForTesting
  protected static void insertWorkersIntoRebalancingTable(
          RpcJobColocatingRebalancer.RebalancingCache rebalancingCache,
          List<Integer> workersNeededPerPartition,
          final Map<Long, StoredWorker> workerMap,
          int numberOfPartition,
          RebalancerConfiguration rebalancerConfiguration,
          Map<String, Integer> jobGroupToPartitionMap,
          final Map<String, RebalancingJobGroup> jobGroupMap) {

    // 1.) Add all workers for each job group to the rebalancing table, provided workers are still
    // valid
    for (Map.Entry<String, Integer> entry : jobGroupToPartitionMap.entrySet()) {
      String jobGroupId = entry.getKey();
      int partitionIdx = entry.getValue();
      RebalancingJobGroup jobGroup = jobGroupMap.get(jobGroupId);
      Preconditions.checkNotNull(
              jobGroup,
              String.format("Job group id '%s' is missing, should never happen.", jobGroupId));
      for (StoredJob job : jobGroup.getJobs().values()) {
        long workerId = job.getWorkerId();
        if (workerMap.containsKey(workerId)) {
          // putIfAbsent to avoid worker accidentally being in 2 different partitions
          rebalancingCache.putIfAbsent(workerId, partitionIdx);
        }
      }
    }

    // 2.) "availableWorkers" not in rebalancing table are free to be used where needed
    Set<Long> allWorkerIds = new HashSet<>(workerMap.keySet());
    allWorkerIds.removeAll(rebalancingCache.getAllWorkerIds());
    List<Long> availableWorkers = new ArrayList<>(allWorkerIds);

    // 3.) remove workers from partition if there are too many
    freeExtraWorkers(
            rebalancingCache,
            workersNeededPerPartition,
            numberOfPartition,
            rebalancerConfiguration,
            availableWorkers);

    // 4.) assign new workers to partitions that need them from the pool of available workers
    int totalExtraWorkersNeeded = 0;
    for (int partitionIdx = 0; partitionIdx < numberOfPartition; partitionIdx++) {
      if (workersNeededPerPartition.get(partitionIdx)
              > rebalancingCache.getAllWorkerIdsForPartition(partitionIdx).size()) {
        totalExtraWorkersNeeded +=
                (workersNeededPerPartition.get(partitionIdx)
                        - rebalancingCache.getAllWorkerIdsForPartition(partitionIdx).size());
      }
    }
    roundRobinAssignWorkers(
            totalExtraWorkersNeeded,
            workersNeededPerPartition,
            rebalancingCache,
            availableWorkers,
            numberOfPartition);
  }

  private static void freeExtraWorkers(
          RpcJobColocatingRebalancer.RebalancingCache rebalancingCache,
          List<Integer> workersNeededPerPartition,
          int numberOfPartition,
          RebalancerConfiguration rebalancerConfiguration,
          List<Long> availableWorkers) {
    for (int parititionIdx = 0; parititionIdx < numberOfPartition; parititionIdx++) {
      // for partitions that have more workers than expected, we gradually reduce in batch of 10%
      int diff =
              rebalancingCache.getAllWorkersForPartition(parititionIdx).size()
                      - workersNeededPerPartition.get(parititionIdx);
      if (diff >= MINIMUM_WORKER_THRESHOLD) {
        int numberOfWorkersToRemove =
                (int) Math.floor(diff * rebalancerConfiguration.getWorkerToReduceRatio());
        // remove at least 2 workers
        numberOfWorkersToRemove = Math.max(MINIMUM_WORKER_THRESHOLD, numberOfWorkersToRemove);
        logger.info(
                "Need to remove {} workers for partition.",
                numberOfWorkersToRemove,
                StructuredLogging.virtualPartition(parititionIdx),
                StructuredLogging.workloadBasedWorkerCount(
                        workersNeededPerPartition.get(parititionIdx)));
        List<Long> toRemoveWorkerIds =
                removeJobsFromLeastLoadedWorkers(
                        rebalancingCache, parititionIdx, numberOfWorkersToRemove);
        toRemoveWorkerIds.forEach(rebalancingCache::removeWorker);
        availableWorkers.addAll(toRemoveWorkerIds);
        // reset workers needed for this partition to be the same as the current worker size
        workersNeededPerPartition.set(
                parititionIdx,
                rebalancingCache.getAllWorkerIdsForPartition(parititionIdx).size());
      }
    }
  }

  private static void roundRobinAssignWorkers(
          int totalNumberOfWorkersNeeded,
          List<Integer> workersNeededPerPartition,
          RpcJobColocatingRebalancer.RebalancingCache rebalancingCache,
          List<Long> newWorkers,
          int numberOfPartition) {
    int partitionIdx = 0;
    int idleWorkerIdx = 0;
    while (idleWorkerIdx < newWorkers.size() && totalNumberOfWorkersNeeded > 0) {
      if (rebalancingCache.getAllWorkerIdsForPartition(partitionIdx).size()
              < workersNeededPerPartition.get(partitionIdx)) {
        rebalancingCache.put(newWorkers.get(idleWorkerIdx), partitionIdx);
        logger.info(
                "Add worker to partition.",
                StructuredLogging.virtualPartition(partitionIdx),
                StructuredLogging.workerId(newWorkers.get(idleWorkerIdx)));
        idleWorkerIdx += 1;
        totalNumberOfWorkersNeeded -= 1;
      }
      partitionIdx = (partitionIdx + 1) % numberOfPartition;
    }
  }

  private static List<Long> removeJobsFromLeastLoadedWorkers(
          RpcJobColocatingRebalancer.RebalancingCache rebalancingCache,
          int partitionIdx,
          int numberOfWorkersToRemove) {
    List<RebalancingWorkerWithSortedJobs> workers =
            rebalancingCache.getAllWorkersForPartition(partitionIdx);
    workers.sort(RebalancingWorkerWithSortedJobs::compareTo);
    numberOfWorkersToRemove = Math.min(workers.size(), numberOfWorkersToRemove);
    return workers
            .subList(0, numberOfWorkersToRemove)
            .stream()
            .map(RebalancingWorkerWithSortedJobs::getWorkerId)
            .collect(Collectors.toList());
  }

  // round up to a nearest target number to
  //    1) avoid frequent rescale
  //    2) use worker number with pattern instead of random number
  static int roundUpToNearestNumber(int number, int targetNumber) {
    return ((number + targetNumber - 1) / targetNumber) * targetNumber;
  }
}
