package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import static com.uber.data.kafka.consumerproxy.controller.rebalancer.RebalancerCommon.TARGET_UNIT_NUMBER;
import static com.uber.data.kafka.consumerproxy.controller.rebalancer.RebalancerCommon.roundUpToNearestNumber;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.data.kafka.datatransfer.controller.rebalancer.JobPodPlacementProvider;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.m3.tally.Scope;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  private static final String POD_TAG = "pod";

  private static final String COLOCATING_REBALANCER_SUB_SCOPE = "colocating_rebalancer";

  protected final RebalancerConfiguration rebalancerConfiguration;

  private final Scope scope;

  private final Map<String, RebalancingWorkerTable> rebalancingWorkerTableMap;
  private final JobGroupAndWorkerPodifier jobGroupAndWorkerPodifier;
  private final JobPodPlacementProvider jobPodPlacementProvider;
  private final double placementWorkerScaleHardLimit;

  public RpcJobColocatingRebalancer(
      Scope scope,
      RebalancerConfiguration rebalancerConfiguration,
      Scalar scalar,
      HibernatingJobRebalancer hibernatingJobRebalancer,
      JobPodPlacementProvider jobPodPlacementProvider) {
    super(scope, rebalancerConfiguration, scalar, hibernatingJobRebalancer);
    this.rebalancerConfiguration = rebalancerConfiguration;
    this.jobPodPlacementProvider = jobPodPlacementProvider;
    this.scope = scope;
    this.rebalancingWorkerTableMap = new HashMap<>();
    this.jobGroupAndWorkerPodifier = new JobGroupAndWorkerPodifier(jobPodPlacementProvider, scope);
    this.placementWorkerScaleHardLimit = rebalancerConfiguration.getPlacementWorkerScaleHardLimit();
  }

  @Override
  public void computeWorkerId(
      final Map<String, RebalancingJobGroup> jobGroupMap, final Map<Long, StoredWorker> workerMap) {
    rebalancingWorkerTableMap.clear();
    // isolate jobs and workers within the same pod and do rebalance separately
    List<PodAwareRebalanceGroup> podAwareRebalanceGroups =
        jobGroupAndWorkerPodifier.podifyJobGroupsAndWorkers(jobGroupMap, workerMap);
    int totalNumberRequestedWorkers = 0;
    for (PodAwareRebalanceGroup podAwareRebalanceGroup : podAwareRebalanceGroups) {
      RebalancingWorkerTable rebalancingWorkerTable = new RebalancingWorkerTable();
      rebalancingWorkerTableMap.put(podAwareRebalanceGroup.getPod(), rebalancingWorkerTable);

      Map<String, Integer> jobGroupToPartitionMap = new HashMap<>();
      List<Integer> workerNeededPerPartition =
          RebalancerCommon.generateWorkerVirtualPartitions(
              podAwareRebalanceGroup,
              rebalancerConfiguration,
              jobGroupToPartitionMap,
              rebalancingWorkerTable);

      // step 1: move job/jobGroup to correct virtual partitions first
      Map<Long, StaleWorkerReplacement> toBeMovedStaleJobs = new HashMap<>();
      assignJobsToCorrectVirtualPartition(
          podAwareRebalanceGroup,
          jobGroupMap,
          toBeMovedStaleJobs,
          jobGroupToPartitionMap,
          rebalancingWorkerTable);

      // step 2: handle jobs on stale workers after workload adjustment
      handleJobsOnStaleWorkers(toBeMovedStaleJobs, rebalancingWorkerTable);

      // step 3: adjust the workload of each worker one by one
      ensureWorkersLoadBalanced(rebalancingWorkerTable, podAwareRebalanceGroup.getPod());

      // step 4: convert to the final result
      for (RebalancingWorkerWithSortedJobs worker : rebalancingWorkerTable.getAllWorkers()) {
        List<RebalancingJob> jobs = worker.getAllJobs();
        jobs.forEach(job -> job.setWorkerId(worker.getWorkerId()));
      }

      // step 5: emit metrics
      emitMetrics(
          rebalancingWorkerTable, workerNeededPerPartition, podAwareRebalanceGroup.getPod());

      int totalRequestedNumberOfWorkerPerPod =
          workerNeededPerPartition.stream().mapToInt(Integer::intValue).sum();
      totalNumberRequestedWorkers += totalRequestedNumberOfWorkerPerPod;
    }

    // step 6: emit overall workers target count
    totalNumberRequestedWorkers =
        roundUpToNearestNumber(totalNumberRequestedWorkers, TARGET_UNIT_NUMBER);
    scope.gauge(MetricNames.WORKERS_TARGET).update(totalNumberRequestedWorkers);
  }

  @Override
  public void computeJobConfiguration(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {
    List<PodAwareRebalanceGroup> podAwareRebalanceGroups =
        jobGroupAndWorkerPodifier.podifyJobGroupsAndWorkers(jobGroups, workers);

    for (PodAwareRebalanceGroup podAwareRebalanceGroup : podAwareRebalanceGroups) {
      String pod = podAwareRebalanceGroup.getPod();
      Optional<Double> maybeFlowControlRatio =
          jobPodPlacementProvider.getMaybeReservedFlowControlRatioForPods(pod);
      for (Map.Entry<String, List<StoredJob>> groupIdToJobsPair :
          podAwareRebalanceGroup.getGroupIdToJobs().entrySet()) {
        RebalancingJobGroup rebalancingJobGroup = jobGroups.get(groupIdToJobsPair.getKey());
        Preconditions.checkNotNull(rebalancingJobGroup);
        List<StoredJob> allJobsInPod = groupIdToJobsPair.getValue();
        if (allJobsInPod.isEmpty()) {
          // this will not happen, but warn just in case
          logger.warn("No jobs for pod", StructuredLogging.jobPod(pod));
          continue;
        }

        double scalePerJobInPod =
            rebalancingJobGroup.getScale().orElse(Scalar.ZERO)
                / rebalancingJobGroup.getJobs().size();
        double flowControlRatio = maybeFlowControlRatio.orElse(1.0);
        double messagePerSecondPerJobInPod =
            rebalancingJobGroup.getJobGroup().getFlowControl().getMessagesPerSec()
                * flowControlRatio
                / allJobsInPod.size();
        double bytePerSecondPerJobInPod =
            rebalancingJobGroup.getJobGroup().getFlowControl().getBytesPerSec()
                * flowControlRatio
                / allJobsInPod.size();
        double maxInflightPerJobInPod =
            rebalancingJobGroup.getJobGroup().getFlowControl().getMaxInflightMessages()
                * flowControlRatio
                / allJobsInPod.size();

        FlowControl flowControlForJobInPod =
            FlowControl.newBuilder()
                .setBytesPerSec(bytePerSecondPerJobInPod)
                .setMessagesPerSec(messagePerSecondPerJobInPod)
                .setMaxInflightMessages(maxInflightPerJobInPod)
                .build();

        for (StoredJob job : allJobsInPod) {
          rebalancingJobGroup.updateJob(
              job.getJob().getJobId(),
              job.toBuilder()
                  .setJob(
                      // use the job util that merges a new job group with the old job.
                      Rebalancer.mergeJobGroupAndJob(
                              rebalancingJobGroup.getJobGroup(), job.getJob())
                          .setFlowControl(
                              flowControlForJobInPod) // override per partition flow control
                          .build())
                  .setScale(scalePerJobInPod)
                  .build());
        }
      }
    }
  }

  private void assignJobsToCorrectVirtualPartition(
      PodAwareRebalanceGroup podAwareRebalanceGroup,
      Map<String, RebalancingJobGroup> jobGroupMap,
      Map<Long, StaleWorkerReplacement> toBeMovedStaleJobs,
      Map<String, Integer> jobGroupToPartitionMap,
      RebalancingWorkerTable rebalancingWorkerTable) {
    for (Map.Entry<String, List<StoredJob>> entry :
        podAwareRebalanceGroup.getGroupIdToJobs().entrySet()) {
      String jobGroupId = entry.getKey();
      Preconditions.checkArgument(jobGroupToPartitionMap.containsKey(jobGroupId));
      Preconditions.checkArgument(jobGroupMap.containsKey(jobGroupId));

      long partitionIdxForGroup = jobGroupToPartitionMap.get(jobGroupId);

      for (StoredJob job : entry.getValue()) {
        if (job.getState() != JobState.JOB_STATE_RUNNING) {
          continue;
        }

        long currentWorkerId = job.getWorkerId();
        // job is not on the correct worker
        if (!rebalancingWorkerTable.isWorkerIdValid(currentWorkerId)
            || !rebalancingWorkerTable
                .getAllWorkerIdsForPartition(partitionIdxForGroup)
                .contains(currentWorkerId)) {
          toBeMovedStaleJobs.putIfAbsent(
              partitionIdxForGroup, new StaleWorkerReplacement(partitionIdxForGroup));
          toBeMovedStaleJobs
              .get(partitionIdxForGroup)
              .addRebalancingJob(new RebalancingJob(job, jobGroupMap.get(jobGroupId)));
        } else {
          rebalancingWorkerTable
              .getRebalancingWorkerWithSortedJobs(currentWorkerId)
              .addJob(new RebalancingJob(job, jobGroupMap.get(jobGroupId)));
        }
      }
    }
  }

  private void handleJobsOnStaleWorkers(
      Map<Long, StaleWorkerReplacement> toBeMovedStaleJobs,
      RebalancingWorkerTable rebalancingWorkerTable) {
    int totalStaleJobs = 0;
    for (Map.Entry<Long, StaleWorkerReplacement> entry : toBeMovedStaleJobs.entrySet()) {
      long partitionIdx = entry.getKey();
      List<RebalancingJob> allJobsToMove = entry.getValue().storedJobs;
      totalStaleJobs += allJobsToMove.size();
      // always start from the least loaded worker
      PriorityQueue<RebalancingWorkerWithSortedJobs> candidateWorkers = new PriorityQueue<>();
      candidateWorkers.addAll(rebalancingWorkerTable.getAllWorkersForPartition(partitionIdx));
      // TODO emit metric if candidate worker for a partition is empty
      // TODO: can consider move these jobs to other workers for short mitigation
      if (candidateWorkers.isEmpty()) {
        // The probability of this is low
        logger.warn(
            "There is no workers for the partition",
            StructuredLogging.virtualPartition(partitionIdx));
        continue;
      }

      allJobsToMove.sort(RebalancingJob::compareTo);

      allJobsToMove.forEach(
          job -> {
            // there should be always a worker
            Preconditions.checkArgument(!candidateWorkers.isEmpty());
            RebalancingWorkerWithSortedJobs leastLoadedWorker = candidateWorkers.poll();
            leastLoadedWorker.addJob(job);
            candidateWorkers.add(leastLoadedWorker);
          });
    }
    scope
        .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
        .gauge(MetricNames.STALE_JOB_COUNT)
        .update(totalStaleJobs);
  }

  private void ensureWorkersLoadBalanced(
      RebalancingWorkerTable rebalancingWorkerTable, String pod) {
    for (long partitionIdx : rebalancingWorkerTable.getAllPartitions()) {
      List<RebalancingWorkerWithSortedJobs> allWorkersInPartition =
          rebalancingWorkerTable.getAllWorkersForPartition(partitionIdx);
      // TODO: emit metric if the workers within partition is empty
      List<RebalancingWorkerWithSortedJobs> allWorkers = new ArrayList<>(allWorkersInPartition);
      allWorkers.sort(RebalancingWorkerWithSortedJobs::compareTo);

      int numberOfOverloadingWorker = 0;
      int numberOfWorkerHittingWorkloadHardLimit = 0;
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
              adjustJobsOnWorker(
                  rebalancingWorkerTable, rebalancingWorkerWithSortedJobs, workerIdx, allWorkers);
          if (!adjustedLoad) {
            numberOfUnadjustedWorker += 1;
            logger.warn(
                "Worker is overloaded after adjusting workload.",
                StructuredLogging.workerId(rebalancingWorkerWithSortedJobs.getWorkerId()),
                StructuredLogging.count(rebalancingWorkerWithSortedJobs.getNumberOfJobs()),
                StructuredLogging.workloadScale(rebalancingWorkerWithSortedJobs.getLoad()),
                StructuredLogging.virtualPartition(partitionIdx));
            if (rebalancingWorkerWithSortedJobs.getLoad() > placementWorkerScaleHardLimit) {
              numberOfWorkerHittingWorkloadHardLimit += 1;
            }
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

      scope
          .subScope(COLOCATING_REBALANCER_SUB_SCOPE)
          .tagged(ImmutableMap.of(VIRTUAL_PARTITION_TAG, Long.toString(partitionIdx), POD_TAG, pod))
          .gauge(MetricNames.HIT_WORKLOAD_HARDLIMIT_WORKER)
          .update(numberOfWorkerHittingWorkloadHardLimit);
    }
  }

  private boolean adjustJobsOnWorker(
      RebalancingWorkerTable rebalancingWorkerTable,
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
        if (canMoveWorkloadToWorker(otherWorker, toBeMovedJob)) {
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

  /**
   * There are two conditions that we can move a workload to a worker 1. After taking the workload,
   * the total workload is within the hard limit and number of jobs is within the job limit Or 2.
   * The worker is empty
   */
  private boolean canMoveWorkloadToWorker(
      RebalancingWorkerWithSortedJobs otherWorker, RebalancingJob toBeMovedJob) {
    if (otherWorker.getLoad() + toBeMovedJob.getLoad() <= placementWorkerScaleHardLimit
        && otherWorker.getNumberOfJobs() + 1
            <= rebalancerConfiguration.getMaxJobNumberPerWorker()) {
      return true;
    }

    if (otherWorker.getNumberOfJobs() == 0) {
      return true;
    }

    return false;
  }

  private boolean isWorkerUnderLoadLimit(RebalancingWorkerWithSortedJobs worker) {
    return (worker.getLoad() <= placementWorkerScaleHardLimit
            && worker.getNumberOfJobs() <= rebalancerConfiguration.getMaxJobNumberPerWorker())
        || (worker.getNumberOfJobs() == 1);
  }

  private void emitMetrics(
      RebalancingWorkerTable rebalancingWorkerTable,
      List<Integer> workerNeededPerPartition,
      String pod) {
    Map<String, String> scopeTagsWithPod = new HashMap<>();
    scopeTagsWithPod.put(POD_TAG, pod);
    int usedWorkers = 0;
    for (RebalancingWorkerWithSortedJobs worker : rebalancingWorkerTable.getAllWorkers()) {
      if (worker.getNumberOfJobs() != 0) {
        usedWorkers += 1;
      }
    }
    scope.tagged(scopeTagsWithPod).gauge(MetricNames.USED_WORKER_COUNT).update(usedWorkers);

    int totalNumberOfWorkersStillNeeded = 0;
    for (long partitionIdx : rebalancingWorkerTable.getAllPartitions()) {
      usedWorkers = 0;
      Map<String, String> partitionTags = new HashMap<>();
      partitionTags.put(VIRTUAL_PARTITION_TAG, Long.toString(partitionIdx));
      partitionTags.putAll(scopeTagsWithPod);
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
        Map<String, String> workerTags = new HashMap<>();
        workerTags.put(VIRTUAL_PARTITION_TAG, Long.toString(partitionIdx));
        workerTags.put(MetricNames.WORKER_IDX, Long.toString(worker.getWorkerId()));
        workerTags.put(POD_TAG, pod);
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
        .tagged(scopeTagsWithPod)
        .gauge(MetricNames.EXTRA_WORKERS_UNFULFILLED)
        .update(totalNumberOfWorkersStillNeeded);
  }

  @VisibleForTesting
  Map<String, RebalancingWorkerTable> getRebalancingTable() {
    return rebalancingWorkerTableMap;
  }

  private static class StaleWorkerReplacement {
    private final List<RebalancingJob> storedJobs;
    private final long virtualPartitionIndex;

    StaleWorkerReplacement(long virtualPartitionIndex) {
      this.storedJobs = new ArrayList<>();
      this.virtualPartitionIndex = virtualPartitionIndex;
    }

    void addRebalancingJob(RebalancingJob rebalancingJob) {
      this.storedJobs.add(rebalancingJob);
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

    private static final String HIT_WORKLOAD_HARDLIMIT_WORKER = "hit.hardlimit.worker.number";

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
