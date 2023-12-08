package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

/** Rebalancer utility functions */
class RebalancerCommon {

  // this is the unit number to round up when calculating the expecetd worker number for the cluster
  static final int TARGET_UNIT_NUMBER = 5;
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

  static RpcJobColocatingRebalancer.RebalancingWorkerTable generateWorkerVirtualPartitions(
      final Map<String, RebalancingJobGroup> jobGroupMap,
      final Map<Long, StoredWorker> workerMap,
      RebalancerConfiguration rebalancerConfiguration,
      Map<String, Integer> jobGroupToPartitionMap) {
    int numberOfPartition = rebalancerConfiguration.getNumberOfVirtualPartitions();

    // calculate the total workload of job group per partition
    List<Double> workloadByPartitionList =
        new ArrayList<>(Collections.nCopies(numberOfPartition, 0.0));
    List<Integer> jobCountByPartitionList =
        new ArrayList<>(Collections.nCopies(numberOfPartition, 0));
    for (RebalancingJobGroup jobGroup : jobGroupMap.values()) {
      long hashValue =
          Math.abs(
              jobGroup.getJobGroup().getJobGroupId().hashCode()
                  % rebalancerConfiguration.getMaxAssignmentHashValueRange());
      int partitionIdx = (int) (hashValue % numberOfPartition);
      jobGroupToPartitionMap.put(jobGroup.getJobGroup().getJobGroupId(), partitionIdx);

      workloadByPartitionList.set(
          partitionIdx,
          workloadByPartitionList.get(partitionIdx) + jobGroup.getScale().orElse(0.0));
      jobCountByPartitionList.set(
          partitionIdx,
          jobCountByPartitionList.get(partitionIdx) + jobGroup.getJobs().values().size());
    }

    // round the workload per parittion to nearliest multiple of 0.5, so that we don't readjust the
    // worker frequently.
    // TODO: cache the worker->partition mapping to maintain
    // stickiness(https://t3.uberinternal.com/browse/KAFEP-4627)
    workloadByPartitionList =
        workloadByPartitionList
            .stream()
            .map(workload -> roundHalf(workload))
            .collect(Collectors.toList());
    double totalWorkload = workloadByPartitionList.stream().mapToDouble(Double::doubleValue).sum();

    // calculate how many workers are needed per partition based on workload
    List<Integer> workersNeededPerPartition = new ArrayList<>();
    int totalAvailableWorkers = workerMap.keySet().size();
    int sumNeededNumberOfWorker = 0;
    for (int idx = 0; idx < workloadByPartitionList.size(); idx++) {
      int expectedNumberOfWorkerPerWorkload =
          (int) (workloadByPartitionList.get(idx) / totalWorkload * totalAvailableWorkers);
      int expectedNumberOfWorkerPerJobCount =
          (jobCountByPartitionList.get(idx)
                  + rebalancerConfiguration.getMaxJobNumberPerWorker()
                  - 1)
              / rebalancerConfiguration.getMaxJobNumberPerWorker();
      int neededNumberOfWorker =
          Math.max(expectedNumberOfWorkerPerWorkload, expectedNumberOfWorkerPerJobCount);
      if (idx == workloadByPartitionList.size() - 1) {
        neededNumberOfWorker = totalAvailableWorkers - sumNeededNumberOfWorker;
      } else {
        sumNeededNumberOfWorker += neededNumberOfWorker;
      }
      workersNeededPerPartition.add(neededNumberOfWorker);
    }

    // sort worker by hashvalue
    List<Pair<Long, Long>> workerIdAndHash = new ArrayList<>();
    for (Long workerId : workerMap.keySet()) {
      StoredWorker worker = workerMap.get(workerId);
      String hashKey =
          String.format("%s:%s", worker.getNode().getHost(), worker.getNode().getPort());
      long hashValue = Math.abs(Hashing.md5().hashString(hashKey, StandardCharsets.UTF_8).asLong());

      workerIdAndHash.add(
          Pair.of(workerId, hashValue % rebalancerConfiguration.getMaxAssignmentHashValueRange()));
    }

    workerIdAndHash.sort(Comparator.comparing(Pair::getRight));

    // assign workers to partition
    RpcJobColocatingRebalancer.RebalancingWorkerTable rebalancingWorkerTable =
        new RpcJobColocatingRebalancer.RebalancingWorkerTable();

    int workerIdx = 0;
    for (int partitionIdx = 0; partitionIdx < numberOfPartition; partitionIdx++) {
      for (int idx = 0; idx < workersNeededPerPartition.get(partitionIdx); idx++) {
        Pair<Long, Long> worker = workerIdAndHash.get(workerIdx++);
        rebalancingWorkerTable.put(worker.getLeft(), partitionIdx);
      }
    }

    return rebalancingWorkerTable;
  }

  // round up to a nearest target number to
  //    1) avoid frequent rescale
  //    2) use worker number with pattern instead of random number
  static int roundUpToNearestNumber(int number, int targetNumber) {
    return ((number + targetNumber - 1) / targetNumber) * targetNumber;
  }

  private static double roundHalf(double number) {
    double diff = number - (int) number;
    if (diff < 0.25) {
      return (int) number;
    } else if (diff < 0.75) {
      return (int) number + 0.5;
    } else {
      return (int) number + 1;
    }
  }
}
