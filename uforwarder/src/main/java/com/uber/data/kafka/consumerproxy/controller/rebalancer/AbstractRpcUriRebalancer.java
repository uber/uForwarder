package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.WorkerUtils;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.m3.tally.Scope;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractRpcUriRebalancer assigns jobs to workers based on the RpcDispatcher Uri field.
 *
 * <p>The assignment algorithm for each job toBeAssigned is:
 *
 * <ol>
 *   <li>determine the set of workers currently assigned to this rpc uri
 *   <li>determine the set of spare workers (not assigned to any rpc uri)
 *   <li>add workers from spare workers to workers assigned to this rpc uri up to numWorkersPerUri
 *   <li>Round robin assign jobs for this rpc uri to the set of workers for this uri.
 * </ol>
 *
 * <p>It is possible that a job is accepted (runJob is called) when the spare worker count = 0. In
 * order to avoid, we emit metrics for the spare worker count so that we can ensure that the spare
 * worker count is larger than some fixed threshold. If the spare workers are less than the
 * threshold, an operator must provision more capacity via uDeploy UI.
 */
abstract class AbstractRpcUriRebalancer implements Rebalancer {

  private static final Logger logger = LoggerFactory.getLogger(AbstractRpcUriRebalancer.class);
  private static final int TARGET_NUMBER = 5;
  private static final double CAPACITY_PER_WORKER = 1.0;
  protected RebalancerConfiguration config;
  private final Scalar scalar;
  private final Scope scope;
  private final HibernatingJobRebalancer hibernatingJobRebalancer;

  AbstractRpcUriRebalancer(
      Scope scope,
      RebalancerConfiguration config,
      Scalar scalar,
      HibernatingJobRebalancer hibernatingJobRebalancer) {
    this.scope = scope.tagged(ImmutableMap.of("rebalancerType", "AbstractRpcUriRebalancer"));
    this.config = config;
    this.scalar = scalar;
    this.hibernatingJobRebalancer = hibernatingJobRebalancer;
  }

  /**
   * Computes worker for job groups 1. compute worker for running job groups 2. compute worker of
   * hibernating job groups
   *
   * @param jobGroupMap is a map of current job groups key by job group Id
   * @param workerMap is a map of current workers.
   */
  @Override
  public void computeWorkerId(
      final Map<String, RebalancingJobGroup> jobGroupMap, final Map<Long, StoredWorker> workerMap) {

    // split jobs groups into two groups: running job groups and hibernating job groups
    Map<Boolean, List<RebalancingJobGroup>> JobGroupsByType =
        jobGroupMap
            .values()
            .stream()
            .collect(Collectors.groupingBy(jobGroup -> isHibernatingJobGroup(jobGroup)));

    Map<String, SortedSet<RebalancingWorker>> reservedAssignment =
        computeWorkerIdOfRunningJobs(
            JobGroupsByType.getOrDefault(false, Collections.EMPTY_LIST), workerMap);

    Set<Long> usedRunningWorkers = new HashSet<>();
    reservedAssignment
        .values()
        .forEach(
            sortedSet ->
                usedRunningWorkers.addAll(
                    sortedSet
                        .stream()
                        .map(RebalancingWorker::getWorkerId)
                        .collect(Collectors.toList())));

    Map<Long, StoredWorker> spareWorkerMap =
        workerMap
            .entrySet()
            .stream()
            .filter(entry -> !usedRunningWorkers.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Collection<Long> usedHibernatingWorkers =
        hibernatingJobRebalancer.computeWorkerId(
            JobGroupsByType.getOrDefault(true, Collections.EMPTY_LIST), spareWorkerMap);

    Set<Long> usedWorkers =
        ImmutableSet.<Long>builder()
            .addAll(usedRunningWorkers)
            .addAll(usedHibernatingWorkers)
            .build();
    // emit metrics for used workers and spared workers
    // reservedAssignment contains the comprehensive worker assignment information
    // usedWorkers set in table.columnSet is incomplete because some workers might assigned to a rpc
    // uri, but don't have job assignment, they won't show up on table.columnSet.
    // KAFEP-910
    emitMetrics(workerMap.keySet(), usedWorkers);
  }

  /**
   * recomputes the assignment of running jobs to workers.
   *
   * @param jobGroups is a list of RebalancingJobGroup.
   * @param workerMap is a map of current workers.
   *     <p>The fundamental abstraction is a Guava Table<Long,Long,RebalancingJob>, which contains a
   *     tuple (job_id, worker_id, job). This data structure contains the current assignment as a
   *     (job_id, worker_id) pair. To ensure extensibility of assignment logic, this is the shared
   *     data structure that is passed between helper methods. Any change to the assignment must be
   *     reflected as an update to the (job_id, worker_id) pair in this table. Note: the table must
   *     contain exactly one tuple for each job_id pair.
   */
  private Map<String, SortedSet<RebalancingWorker>> computeWorkerIdOfRunningJobs(
      final List<RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workerMap) {
    // Create the shared data structures that store the assignment state:
    // 1. Table (rowKey:jobId, columnKey:workerId, value:rebalancingJob)
    // 2. Immutable Set of worker ids and job ids.
    HashBasedTable<Long, Long, RebalancingJob> table = RebalancerCommon.createTable(jobGroups);
    Set<Long> workerIds = Collections.unmodifiableSet(workerMap.keySet());

    // defensively unset jobs that are assigned to stale workers
    RebalancerCommon.ensureValidWorkerId(table, workerIds);

    // move jobs around so that we use a minimum number of workers where possible
    // TODO: moving batch jobs around might not be a good idea, because the same job will start
    //  consuming from the start offset again.
    //  This is not a problem as for now, because
    //  (1) each dlq topic only have one partition, thus only one job will be generated
    //  (2) for the same uri, the probability that users merge different dlq topics at the same time
    //  is relatively low.
    minimizeWorkers(table);

    // ensure all jobs as assigned, dynamically scaling out workers where required.
    // we assign worker to job in 2 steps to support new job in higher priority
    // first step make sure all new jobs assigned with worker
    // second step try to scale out old job when there is spare worker
    ensureJobsAssigned(table, workerIds, Collections.emptyMap());

    // make sure workers have assignment by unpin job from worker if any of the worker from the same
    // uri don't have workload
    ImmutableMap.Builder<Long, Long> jobToPreferredWorkerBuilder = ImmutableMap.builder();
    jobToPreferredWorkerBuilder.putAll(ensureWorkerGetAssigned(table, workerIds));
    // make sure workers are under loaded by unpin job from worker temporary
    jobToPreferredWorkerBuilder.putAll(ensureWorkerUnderloaded(table));

    // ensure all jobs as assigned, this step make sure all unpinned job get assigned
    // to value stable assignment, assign job to original worker if no enough spare worker
    return ensureJobsAssigned(table, workerIds, jobToPreferredWorkerBuilder.build());
  }

  @Override
  public void computeLoad(final Map<String, RebalancingJobGroup> jobGroups) {
    jobGroups
        .values()
        .forEach(
            jobGroup -> {
              double defaultScale =
                  messageRateToLoad(jobGroup.getJobGroup().getFlowControl().getMessagesPerSec());
              scalar.apply(jobGroup, defaultScale);
            });
  }

  /**
   * Minimizes the number of workers that are used by merging workers together.
   *
   * <p>This implementation adheres to the numWorkerPerUri config as a minimum value so it will not
   * shrink workers below the configured value even if it could.
   *
   * <p>This implementation looks for full workers to merge together. If a worker has multiple jobs,
   * it does not attempt to split the jobs across multiple workers. The latter could find a more
   * efficient bin packing and is a potential future improvement.
   */
  private void minimizeWorkers(HashBasedTable<Long, Long, RebalancingJob> table) {
    Map<String, Map<Long, Double>> uriToLoadPerWorker = new HashMap<>();
    for (RebalancingJob job : table.values()) {
      String uri = job.getRpcUri();
      uriToLoadPerWorker
          .computeIfAbsent(uri, o -> new HashMap<>())
          .compute(
              job.getWorkerId(),
              (aLong, aDouble) -> aDouble == null ? job.getLoad() : aDouble + job.getLoad());
    }

    Map<Long, Map<Long, RebalancingJob>> jobsPerWorkerId = table.columnMap();
    for (Map<Long, Double> loadPerWorker : uriToLoadPerWorker.values()) {
      if (loadPerWorker.size() <= config.getNumWorkersPerUri()) {
        // do not minimize workers below numWorkersPerUri config.
        continue;
      }
      ImmutableList<Long> workerIdsIncreasingLoad =
          ImmutableList.copyOf(
              ImmutableMap.<Long, Double>builder()
                  .orderEntriesByValue(
                      Double::compare) // WARNING: orderEntriesByValue is marked @Beta
                  .putAll(loadPerWorker)
                  .build()
                  .keySet());
      ImmutableList<Long> workerIdsDecreasingLoad = workerIdsIncreasingLoad.reverse();
      workerIdsIncreasingLoad.forEach(
          thisWorkerId -> {
            // O(n) lookup from max loaded worker to find something to merge
            // the current implementation merges full workers. This could be improved to split a job
            // on
            // a single worker over multiple other workers.
            Double thisLoad = loadPerWorker.get(thisWorkerId);
            Preconditions.checkNotNull(
                thisLoad, "load should exist for worker_id since we built iterator from keyset");
            for (long otherWorkerId : workerIdsDecreasingLoad) {
              if (otherWorkerId == thisWorkerId) {
                // no need to consider otherWorkerIds < thisWorkerId because those have already
                // been shrunk if possible.
                break;
              }
              Double otherLoad = loadPerWorker.get(otherWorkerId);
              Preconditions.checkNotNull(
                  otherLoad, "load should exist for worker_id since we built iterator from keyset");
              if (otherLoad + thisLoad < CAPACITY_PER_WORKER) {
                jobsPerWorkerId
                    .getOrDefault(thisWorkerId, new HashMap<>())
                    .values()
                    .forEach(
                        job -> {
                          job.setWorkerId(otherWorkerId);
                          table.put(job.getJobId(), otherWorkerId, job);
                          table.remove(job.getJobId(), thisWorkerId);
                        });
                loadPerWorker.put(otherWorkerId, otherLoad + thisLoad);
                loadPerWorker.put(thisWorkerId, 0.0);
                break;
              }
            }
          });
    }
  }

  /**
   * Computes the workers for a URI using the following preference:
   *
   * <ol>
   *   <li>Prefers stable assignment so if a job is assigned to a worker that assignment is
   *       considered stable and retained.
   *   <li>Ensures that each URI receives a minimum of numWorkersPerUri workers.
   * </ol>
   *
   * <p>TODO: while minimzeWorkers shrinks the number of workers where possible, it is still
   * possible that we underprovision the number of workers. In that case, we should respect the
   * numWorkersPerUri as the reserved quota and steal workers from URI that have > numWorkersPerUri
   * so that the reserved numWorkersPerUri is respected.
   */
  private Map<String, SortedSet<RebalancingWorker>> computeWorkersPerUri(
      HashBasedTable<Long, Long, RebalancingJob> table, Set<Long> workerIds) {
    Map<String, Map<Long, Double>> messagesPerSecByWorkerAndUri = new HashMap<>();
    Map<Long, Map<Long, RebalancingJob>> workerIdToJobIdToJobMap = table.columnMap();
    Set<String> uris =
        Collections.unmodifiableSet(
            table.values().stream().map(RebalancingJob::getRpcUri).collect(Collectors.toSet()));

    // create virtual pool per rpc uri
    uris.forEach(uri -> messagesPerSecByWorkerAndUri.put(uri, new HashMap<>()));

    // ensure stable assignment
    for (Map.Entry<Long, Map<Long, RebalancingJob>> assignment :
        workerIdToJobIdToJobMap.entrySet()) {
      long workerId = assignment.getKey();
      // enumerate jobs to make sure worker load aggregated correctly
      for (RebalancingJob job : assignment.getValue().values()) {
        if (job == null || job.getWorkerId() == WorkerUtils.UNSET_WORKER_ID) {
          // no job assigned to this worker
          continue;
        }
        String uri = job.getRpcUri();
        Map<Long, Double> workerIdsForUri = messagesPerSecByWorkerAndUri.get(uri);
        Preconditions.checkNotNull(
            workerIdsForUri,
            "workerIdsForUri should not be null because we prepopulated empty map per uri");
        workerIdsForUri.put(workerId, workerIdsForUri.getOrDefault(workerId, 0.0) + job.getLoad());
      }
    }

    // top up each virtual pool to numWorkersPerUri
    Set<Long> spareWorkerSet =
        Sets.difference(
            workerIds,
            messagesPerSecByWorkerAndUri
                .values()
                .stream()
                .flatMap(e -> e.keySet().stream())
                .collect(Collectors.toSet()));
    // sort by decreasing worker id so that we prefer placement onto newer workers to avoid
    // rebalance thrashing
    List<Long> spareWorkerList = ImmutableList.copyOf(spareWorkerSet).reverse();
    int spareWorkerIndex = 0;
    for (String uri : uris) {
      Map<Long, Double> workerIdsForUri = messagesPerSecByWorkerAndUri.get(uri);
      Preconditions.checkNotNull(
          workerIdsForUri,
          "workerIdsForUri should not be null because we prepopulated empty map per uri");
      while (workerIdsForUri.size() < config.getNumWorkersPerUri()
          && spareWorkerIndex < spareWorkerList.size()) {
        workerIdsForUri.put(spareWorkerList.get(spareWorkerIndex++), 0.0);
      }
      if (workerIdsForUri.size() == 0) {
        logger.error("worker starvation for virtual pool", StructuredLogging.uri(uri));
      } else if (workerIdsForUri.size() < config.getNumWorkersPerUri()) {
        logger.warn(
            "underprovisioned workers for virtual pool",
            StructuredLogging.uri(uri),
            StructuredLogging.count(workerIdsForUri.size()));
      }
    }

    // convert to sorted set
    Map<String, SortedSet<RebalancingWorker>> workersPerUri = new HashMap<>();
    for (Map.Entry<String, Map<Long, Double>> messagesPerSecByWorker :
        messagesPerSecByWorkerAndUri.entrySet()) {
      String uri = messagesPerSecByWorker.getKey();
      workersPerUri.putIfAbsent(uri, new TreeSet<>());
      for (Map.Entry<Long, Double> messagesPerSecPerWorker :
          messagesPerSecByWorker.getValue().entrySet()) {
        long workerId = messagesPerSecPerWorker.getKey();
        double messagesPerSec = messagesPerSecPerWorker.getValue();
        SortedSet<RebalancingWorker> workerSet = workersPerUri.get(uri);
        Preconditions.checkNotNull(
            workerSet, "worker set should not be null because it was just created");
        workerSet.add(new RebalancingWorker(workerId, messagesPerSec));
      }
    }
    return workersPerUri;
  }

  /**
   * Ensures that jobs are all assigned to live workers with the following rules:
   *
   * <ol>
   *   <li>Prefers stable assignment so if a job is assigned to a worker that assignment is
   *       considered stable and retained.
   *   <li>Ensures that each URI receives a minimum of numWorkersPerUri workers.
   *   <li>If there are spare workers and messagesPerSec quota for a job exceeds the configured
   *       messagesPerSecPerWorker, additional workers are used.
   * </ol>
   */
  private Map<String, SortedSet<RebalancingWorker>> ensureJobsAssigned(
      HashBasedTable<Long, Long, RebalancingJob> table,
      Set<Long> workerIds,
      Map<Long, Long> jobToOriginalWorker) {
    Set<Long> jobIds = ImmutableSet.copyOf(table.rowKeySet());

    // compute the reserved assignment that respects stable assignment and numWorkersPerUri.
    Map<String, SortedSet<RebalancingWorker>> reservedAssignment =
        computeWorkersPerUri(table, workerIds);

    // compute the spare worker set that will be used for dynamic scaling of number of workers based
    // on messagesPerSecPerWorker config.
    Set<Long> spareWorkerSet =
        Sets.difference(
            workerIds,
            reservedAssignment
                .values()
                .stream()
                .flatMap(Collection::stream)
                .map(RebalancingWorker::getWorkerId)
                .collect(Collectors.toSet()));
    // sort by decreasing worker id so that we prefer placement onto newer workers to avoid
    // rebalance thrashing
    List<Long> spareWorkerList = ImmutableList.copyOf(spareWorkerSet).reverse();
    int spareWorkerIndex = 0;

    for (Long jobId : jobIds) {
      Map<Long, RebalancingJob> row = table.row(jobId);
      long workerId = row.keySet().iterator().next();
      if (workerId == WorkerUtils.UNSET_WORKER_ID) {
        // for jobs assigned to UNSET_WORKER_ID, try to find an assignment by
        RebalancingJob job = row.get(workerId);
        Preconditions.checkNotNull(
            job,
            "rebalancingJob should not be null because Guava table guarantees >=1 column (worker_id) per row");
        String uri = job.getRpcUri();
        double load = job.getLoad();
        // try to place onto a worker that is already used for this rpc uri
        // the rebalancing workers are sorted in ascending order so it is sufficient to check the
        // first one
        SortedSet<RebalancingWorker> workersForUri =
            reservedAssignment.getOrDefault(uri, new TreeSet<>());
        Optional<RebalancingWorker> worker = Optional.empty();
        if (workersForUri.size() > 0
            && (workersForUri.first().getLoad() + load < CAPACITY_PER_WORKER
                || workersForUri.first().getLoad() == 0.0)) {
          // workersForUri is sorted by space used so if it doesn't fit onto the first worker, it
          // will not fit onto any other worker.
          worker = Optional.of(workersForUri.first());
          logger.debug(
              "assign job to reserved worker",
              StructuredLogging.uri(uri),
              StructuredLogging.jobId(jobId),
              StructuredLogging.workerId(worker.get().getWorkerId()));
        } else if (spareWorkerIndex < spareWorkerList.size()) {
          // if failed to place onto existing worker, use a spare worker
          worker = Optional.of(new RebalancingWorker(spareWorkerList.get(spareWorkerIndex++), 0.0));
          logger.debug(
              "assign job to spare worker",
              StructuredLogging.uri(uri),
              StructuredLogging.jobId(jobId),
              StructuredLogging.workerId(worker.get().getWorkerId()));
        } else if (jobToOriginalWorker.containsKey(jobId)) {
          // can't find optimal placement for the job, fallback to original worker
          // to make it work, original worker should not change uri in any between process
          long originalWorkerId = jobToOriginalWorker.get(jobId);
          worker =
              workersForUri.stream().filter(w -> w.getWorkerId() == originalWorkerId).findAny();
        } else if (workersForUri.size() > 0) {
          // overload the first worker because there are no spare workers
          worker = Optional.of(workersForUri.first());
          logger.warn(
              "overloading worker due to lack of capacity",
              StructuredLogging.uri(uri),
              StructuredLogging.jobId(job.getJobId()),
              StructuredLogging.workerId(worker.get().getWorkerId()));
        } else {
          // this should not happen b/c we reserve 2 workers per rpc uri
          // if this happens, we need to increase capacity of worker pool
          logger.error(
              "overloading worker due to lack of capacity",
              StructuredLogging.uri(uri),
              StructuredLogging.jobId(job.getJobId()));
        }

        worker.ifPresent(
            w -> {
              workersForUri.remove(w);
              workersForUri.add(w.add(load));
              job.setWorkerId(w.getWorkerId());
              table.remove(jobId, workerId);
              table.put(jobId, w.getWorkerId(), job);
            });
      }
    }
    return reservedAssignment;
  }

  /**
   * Unpin job from worker if the worker is overloaded, return mapping between job and original
   * worker
   *
   * <p>the returned result can be used to recover the relationship when there is no enough spare
   * worker
   *
   * @param table rowKey:jobId, columnKey:workerId, value:rebalancingJob
   * @return mapping from jobId to original worker Id
   */
  private Map<Long, Long> ensureWorkerUnderloaded(
      HashBasedTable<Long, Long, RebalancingJob> table) {

    // calculate mapping from jobId to overloaded worker
    Map<Long, Double> workerToLoad = new HashMap<>();
    Map<Long, Long> result = new HashMap<>();
    for (long jobId : table.rowKeySet()) {
      Map<Long, RebalancingJob> row = table.row(jobId);
      long workerId = row.keySet().iterator().next();
      if (workerId == WorkerUtils.UNSET_WORKER_ID) {
        continue;
      }
      RebalancingJob job = row.get(workerId);
      Preconditions.checkNotNull(
          job,
          "rebalancingJob should not be null because Guava table guarantees >=1 column (worker_id) per row");
      if (workerToLoad.containsKey(workerId)) {
        double load = workerToLoad.get(workerId) + job.getLoad();
        if (load < CAPACITY_PER_WORKER) {
          workerToLoad.put(workerId, load);
        } else {
          result.put(jobId, workerId);
        }
      } else {
        // pin first job to its worker no matter it's overloaded or not
        // to stabilize assignment of over-sized job and prevent cascading re-assign
        workerToLoad.put(workerId, job.getLoad());
      }
    }

    // unpin job from overloaded worker
    for (Map.Entry<Long, Long> jobWorker : result.entrySet()) {
      long jobId = jobWorker.getKey();
      long workerId = jobWorker.getValue();
      RebalancingJob job = table.get(jobId, workerId);
      table.remove(jobId, workerId);
      table.put(jobWorker.getKey(), WorkerUtils.UNSET_WORKER_ID, job);
      // Unpin job from worker, but if spare worker is not enough, we will recover the mapping by
      // func ensureJobsAssigned
      // Anyway, it will trigger status change of job group
      // worker who picked up status change will compare existing job group and new job group to
      // calculate new job to run or to cancel
      // see AbstractKafkaFetcherThread::extractTopicPartitionMap
      job.setWorkerId(WorkerUtils.UNSET_WORKER_ID);
    }

    return ImmutableMap.copyOf(result);
  }

  /**
   * Make sure all the workers have job assignment by unpin job from worker that has most workload
   * and leverage ensureJobsAssigned to assign the workload
   *
   * @param table rowKey:jobId, columnKey:workerId, value:rebalancingJob
   * @param workerIds unique identifier for all the workers
   * @return mapping from jobId to original worker Id
   */
  private Map<Long, Long> ensureWorkerGetAssigned(
      HashBasedTable<Long, Long, RebalancingJob> table, Set<Long> workerIds) {
    HashMap<Long, Long> result = new HashMap<>();
    // compute the reserved assignment that respects stable assignment and numWorkersPerUri.
    Map<String, SortedSet<RebalancingWorker>> reservedAssignment =
        computeWorkersPerUri(table, workerIds);
    for (String uri : reservedAssignment.keySet()) {
      SortedSet<RebalancingWorker> workersForUri =
          reservedAssignment.getOrDefault(uri, new TreeSet<>());

      // no workload on the worker
      if (workersForUri.size() != 0 && workersForUri.first().getLoad() == 0) {
        // reserve workersForUri to make list in decrease order
        List<RebalancingWorker> reversedWorkersForUri = new ArrayList<>();
        workersForUri.stream().forEachOrdered(worker -> reversedWorkersForUri.add(0, worker));
        Long workerToUnload = WorkerUtils.UNSET_WORKER_ID;
        for (RebalancingWorker worker : reversedWorkersForUri) {
          if (worker.getLoad() == 0) {
            continue;
          }
          Map<Long, RebalancingJob> jobs = table.column(worker.getWorkerId());
          if (jobs.size() != 1) {
            workerToUnload = worker.getWorkerId();
            break;
          }
        }

        // No workers match unload criteria, skip worker unload
        if (workerToUnload == WorkerUtils.UNSET_WORKER_ID) {
          logger.warn("fail to find worker to unload");
          return result;
        }

        // unpin workload for selected worker
        Map<Long, RebalancingJob> jobs = table.column(workerToUnload);
        List<Long> jobIds = ImmutableList.copyOf(jobs.keySet());
        for (Long jobId : jobIds) {
          RebalancingJob job = table.get(jobId, workerToUnload);
          table.remove(jobId, workerToUnload);
          table.put(jobId, WorkerUtils.UNSET_WORKER_ID, job);
          job.setWorkerId(WorkerUtils.UNSET_WORKER_ID);
          result.put(jobId, workerToUnload);
        }
      }
    }
    return ImmutableMap.copyOf(result);
  }

  private boolean isHibernatingJobGroup(RebalancingJobGroup jobGroup) {
    return jobGroup.getScale().isPresent() && jobGroup.getScale().get().equals(Scalar.ZERO);
  }

  private void emitMetrics(Set<Long> allWorkers, Set<Long> usedWorkers) {
    // TODO:yayang how to calculate target worker count when used workers are overloaded
    int spareWorkerCount = Sets.difference(allWorkers, usedWorkers).size();
    spareWorkerCount =
        usedWorkers.contains(WorkerUtils.UNSET_WORKER_ID) ? spareWorkerCount - 1 : spareWorkerCount;
    scope.gauge(MetricNames.WORKERS_TOTAL).update(allWorkers.size());
    scope.gauge(MetricNames.WORKERS_SPARE).update(spareWorkerCount);
    scope.gauge(MetricNames.WORKERS_USED).update(allWorkers.size() - spareWorkerCount);

    // emit direct-metric-scaling metrics for worker auto scaling
    // add additional spare worker based on percentage and round up to integer
    int targetWorkerCount =
        usedWorkers.size()
            + (int)
                Math.ceil(
                    usedWorkers.size() * ((double) config.getTargetSpareWorkerPercentage() / 100));
    scope
        .gauge(MetricNames.WORKERS_TARGET)
        .update(
            Math.max(
                config.getMinimumWorkerCount(),
                roundUpToNearestNumber(targetWorkerCount, TARGET_NUMBER)));
  }

  // round up to a nearest target number to
  //    1) avoid frequent rescale
  //    2) use worker number with pattern instead of random number
  int roundUpToNearestNumber(int number, int targetNumber) {
    return ((number + targetNumber - 1) / targetNumber) * targetNumber;
  }

  @VisibleForTesting
  void reportAndLogNewJobStates(final Map<Long, JobState> newJobStates) {
    newJobStates.forEach(
        (jobId, jobState) -> {
          switch (jobState) {
            case JOB_STATE_CANCELED:
              scope.counter(MetricNames.JOB_STATE_CANCELED).inc(1);
              logger.debug("canceling a job", StructuredLogging.jobId(jobId));
              break;
            case JOB_STATE_RUNNING:
              scope.counter(MetricNames.JOB_STATE_RUNNING).inc(1);
              logger.debug("running a job", StructuredLogging.jobId(jobId));
              break;
            case JOB_STATE_FAILED:
              scope.counter(MetricNames.JOB_STATE_FAILED).inc(1);
              logger.debug("failing a job", StructuredLogging.jobId(jobId));
              break;
            case JOB_STATE_UNIMPLEMENTED:
              scope.counter(MetricNames.JOB_STATE_UNIMPLEMENTED).inc(1);
              logger.error("changing a job to unimplemented state", StructuredLogging.jobId(jobId));
              break;
            case JOB_STATE_INVALID:
              scope.counter(MetricNames.JOB_STATE_INVALID).inc(1);
              logger.error("changing a job to invalid state", StructuredLogging.jobId(jobId));
              break;
            default:
              scope.counter(MetricNames.JOB_STATE_UNSUPPORTED).inc(1);
              logger.error("changing a job to unsupported state", StructuredLogging.jobId(jobId));
          }
        });
  }

  private double messageRateToLoad(double messagePerSecond) {
    return messagePerSecond / config.getMessagesPerSecPerWorker();
  }

  @VisibleForTesting
  static class MetricNames {

    static final String WORKERS_SPARE = "rebalancer.workers.spare";
    static final String WORKERS_TARGET = "rebalancer.workers.target";
    private static final String WORKERS_TOTAL = "rebalancer.workers.total";
    private static final String WORKERS_USED = "rebalancer.workers.used";
    private static final String JOB_STATE_CANCELED = "rebalancer.job.state.canceled";
    private static final String JOB_STATE_RUNNING = "rebalancer.job.state.running";
    private static final String JOB_STATE_FAILED = "rebalancer.job.state.failed";
    private static final String JOB_STATE_INVALID = "rebalancer.job.state.invalid";
    private static final String JOB_STATE_UNIMPLEMENTED = "rebalancer.job.state.unimplemented";
    private static final String JOB_STATE_UNSUPPORTED = "rebalancer.job.state.unsupported";

    private MetricNames() {}
  }
}
