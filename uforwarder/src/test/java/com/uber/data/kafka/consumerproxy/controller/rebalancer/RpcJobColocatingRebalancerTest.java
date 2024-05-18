package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.DebugJobRow;
import com.uber.data.kafka.datatransfer.DebugJobsTable;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.ScaleStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.WorkerState;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
public class RpcJobColocatingRebalancerTest extends AbstractRpcUriRebalancerTest {
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {"data/tier3ajobs.json", "data/tier3aworkers.json"},
          {"data/tier5jobs.json", "data/tier5workers.json"}
        });
  }

  private static final String EXTRA_WORKLOAD_ONE_JOB_GROUP_PATH =
      "data/extra_workload_one_job_group.json";

  private static final String EXTRA_WORKLOAD_ONE_JOB_OVERLOAD_WORKERS_GROUP_PATH =
      "data/extra_workload_one_job_group_overload_workers.json";

  private static final String EXTRA_WORKLOAD_JOB_GROUP_SCALE_ONE =
      "data/extra_workload_job_group_scale_one.json";

  private static final String SINGLE_WORKER_HOLDING_LARGE_JOBS_JOBGROUP_PATH =
      "data/workersWithOverloadedJobs.json";

  private static final String SINGLE_WORKER_HOLDING_LARGE_JOBS_WORKER_PATH =
      "data/workersWithOverloadedJobsWorker.json";

  private ImmutableMap<String, RebalancingJobGroup> jsonJobs;

  private ImmutableMap<String, RebalancingJobGroup> jsonJobsForTableCheck;

  private ImmutableMap<String, RebalancingJobGroup> extraWorkloadOneJobGroup;

  private ImmutableMap<String, RebalancingJobGroup> extraWorkloadOneJobGroupOverloadWorkers;

  private ImmutableMap<String, RebalancingJobGroup> extraWorkloadOneJobGroupScaleOne;

  private ImmutableMap<Long, StoredWorker> jsonWorkers;

  private ImmutableMap<String, RebalancingJobGroup> workerOverloadedCaseJobs;
  private ImmutableMap<Long, StoredWorker> workerOverloadedCaseWorkers;

  private Scope mockScope;

  private Scope mockSubScope;

  private Gauge mockGauge;

  private Counter mockCounter;

  // suppress ForbidClassloaderGetResourceInTests as moving to gradle repo
  @SuppressWarnings("ForbidClassloaderGetResourceInTests")
  public RpcJobColocatingRebalancerTest(String jobDataPath, String workerDataPath)
      throws Exception {
    // To load production data for test, download data from
    // https://system-phx.uberinternal.com/udg://kafka-consumer-proxy-master-phx/0:system/jobsJson
    jsonJobs =
        ImmutableMap.copyOf(
            readJsonJobs(
                new InputStreamReader(
                    this.getClass().getClassLoader().getResourceAsStream(jobDataPath))));
    jsonJobsForTableCheck =
        ImmutableMap.copyOf(
            readJsonJobs(
                new InputStreamReader(
                    this.getClass().getClassLoader().getResourceAsStream(jobDataPath))));

    extraWorkloadOneJobGroup =
        ImmutableMap.copyOf(
            readJsonJobs(
                new InputStreamReader(
                    this.getClass()
                        .getClassLoader()
                        .getResourceAsStream(EXTRA_WORKLOAD_ONE_JOB_GROUP_PATH))));

    extraWorkloadOneJobGroupOverloadWorkers =
        ImmutableMap.copyOf(
            readJsonJobs(
                new InputStreamReader(
                    this.getClass()
                        .getClassLoader()
                        .getResourceAsStream(EXTRA_WORKLOAD_ONE_JOB_OVERLOAD_WORKERS_GROUP_PATH))));

    extraWorkloadOneJobGroupScaleOne =
        ImmutableMap.copyOf(
            readJsonJobs(
                new InputStreamReader(
                    this.getClass()
                        .getClassLoader()
                        .getResourceAsStream(EXTRA_WORKLOAD_JOB_GROUP_SCALE_ONE))));

    // To load production data for test, download data from
    // https://system-phx.uberinternal.com/udg://kafka-consumer-proxy-master-phx/0:system/workersJson
    jsonWorkers =
        ImmutableMap.copyOf(
            readJsonWorkers(
                new InputStreamReader(
                    this.getClass().getClassLoader().getResourceAsStream(workerDataPath))));

    workerOverloadedCaseJobs =
        ImmutableMap.copyOf(
            readJsonJobs(
                new InputStreamReader(
                    this.getClass()
                        .getClassLoader()
                        .getResourceAsStream(SINGLE_WORKER_HOLDING_LARGE_JOBS_JOBGROUP_PATH))));

    workerOverloadedCaseWorkers =
        ImmutableMap.copyOf(
            readJsonWorkers(
                new InputStreamReader(
                    this.getClass()
                        .getClassLoader()
                        .getResourceAsStream(SINGLE_WORKER_HOLDING_LARGE_JOBS_WORKER_PATH))));

    mockScope = Mockito.mock(Scope.class);
    mockSubScope = Mockito.mock(Scope.class);
    mockGauge = Mockito.mock(Gauge.class);
    mockCounter = Mockito.mock(Counter.class);
    when(mockScope.subScope(any())).thenReturn(mockSubScope);
    when(mockScope.gauge(any())).thenReturn(mockGauge);
    when(mockScope.counter(any())).thenReturn(mockCounter);
    when(mockSubScope.tagged(any())).thenReturn(mockSubScope);
    when(mockSubScope.gauge(any())).thenReturn(mockGauge);
    when(mockSubScope.counter(any())).thenReturn(mockCounter);
  }

  @Test
  public void testProductionDataCoverageRate() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setMessagesPerSecPerWorker(4000);
    config.setWorkerToReduceRatio(0.9);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);

    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 4);
    Set<Long> usedWorkers = usedWorkers(jobs, workers);
    workers.keySet().removeAll(usedWorkers);
    Assert.assertFalse(usedWorkers.contains(0L));

    Map<Long, Long> jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));
    // rerun the same initial jobs, it should not have diff
    workers = new HashMap<>(jsonWorkers);
    Map<String, RebalancingJobGroup> newJobs = new HashMap<>(jsonJobsForTableCheck);
    runRebalanceToConverge(rebalancer::computeWorkerId, newJobs, workers, 4);
    Map<Long, Long> newJobToWorkerId =
        jobToWorkerId(
            newJobs
                .values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));
    Assert.assertEquals(0, calcDiff(newJobToWorkerId, jobToWorkerId));
  }

  @Override
  @Test
  public void testJsonDataRemoveJobGroup() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setMessagesPerSecPerWorker(4000);
    config.setWorkerToReduceRatio(0.9);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 4);

    Map<Long, Long> jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));

    String groupName = jobs.keySet().stream().findAny().get();
    RebalancingJobGroup group = jobs.get(groupName);
    int nJobs = group.getJobs().size();
    jobs.remove(groupName);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);
    Map<Long, Long> prevJobToWorkerId = jobToWorkerId;
    jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));
    Set<Long> deletedJobIds = deletedJob(prevJobToWorkerId, jobToWorkerId);
    Assert.assertEquals(nJobs, deletedJobIds.size());
  }

  @Override
  @Test
  public void testJsonDataAddWorker() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    config.setWorkerToReduceRatio(0.9);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 4);

    Map<Long, Long> jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));

    Long newWorkerId = 1L;
    putWorkerToMap(workers, newWorkerId);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);
    Map<Long, Long> prevJobToWorkerId = jobToWorkerId;
    jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));
    Assert.assertEquals(0, deletedJob(prevJobToWorkerId, jobToWorkerId).size());
    Set<Long> jobIds = updatedJob(prevJobToWorkerId, jobToWorkerId);
    Assert.assertEquals(0, jobIds.size());
  }

  @Test
  public void testJsonDataWorkloadReduced() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    config.setNumberOfVirtualPartitions(8);
    config.setWorkerToReduceRatio(1.0);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);
    Set<Long> usedWorkers = usedWorkers(jobs, workers);
    Set<Long> idleWorkerIds = new HashSet<>(workers.keySet());
    idleWorkerIds.removeAll(usedWorkers);
    int oldUsedWorkerCount = usedWorkers.size();

    double newTotalWorkload = 0.0;
    for (Map.Entry<String, RebalancingJobGroup> jobGroupEntry : jobs.entrySet()) {
      jobGroupEntry.getValue().updateScale(jobGroupEntry.getValue().getScale().get() / 10.0);
      for (Map.Entry<Long, StoredJob> entry : jobGroupEntry.getValue().getJobs().entrySet()) {
        newTotalWorkload += (entry.getValue().getScale() / 10.0);
        jobGroupEntry
            .getValue()
            .updateJob(
                entry.getKey(),
                StoredJob.newBuilder(entry.getValue())
                    .setScale(entry.getValue().getScale() / 10.0)
                    .build());
      }
    }

    int newWorkerNumber = (int) Math.ceil(newTotalWorkload);

    Map<Long, Long> jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));

    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);
    Map<Long, Long> prevJobToWorkerId = jobToWorkerId;
    jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));
    Assert.assertEquals(0, deletedJob(prevJobToWorkerId, jobToWorkerId).size());

    usedWorkers = usedWorkers(jobs, workers);
    Set<Long> allAvailableWorkers = workers.keySet();
    allAvailableWorkers.removeAll(usedWorkers);
    if (newWorkerNumber < oldUsedWorkerCount - 1) {
      Assert.assertTrue(allAvailableWorkers.size() > idleWorkerIds.size());
    }
  }

  @Override
  @Test
  public void testJsonDataRemoveWorker() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    config.setWorkerToReduceRatio(0.9);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);

    RebalanceSimResult result1 =
        runRebalanceSim(rebalancer::computeWorkerId, this::usedWorkers, jobs, workers, 4);
    Assert.assertTrue(validateOverloadedWorkers(result1, rebalancer.getRebalancingTable()));

    // Find 2 workers to be removed
    Set<Long> workersToBeRemoved =
        jobs.values()
            .stream()
            .map(e -> e.getJobs().values().stream().findFirst().get())
            .map(StoredJob::getWorkerId)
            .distinct()
            .limit(2)
            .collect(Collectors.toSet());

    // collect the jobs which are running on the workers to be removed
    Set<Long> jobsWithRemovedWorkers =
        jobs.values()
            .stream()
            .filter(
                e ->
                    e.getJobs()
                        .values()
                        .stream()
                        .anyMatch(j -> workersToBeRemoved.contains(j.getWorkerId())))
            .flatMap(e -> e.getJobs().values().stream().map(j -> j.getJob().getJobId()))
            .collect(Collectors.toSet());
    Assert.assertEquals(2, workersToBeRemoved.size());
    workers.keySet().removeAll(workersToBeRemoved);

    RebalanceSimResult result2 =
        runRebalanceSim(rebalancer::computeWorkerId, this::usedWorkers, jobs, workers, 2);
    Assert.assertTrue(validateOverloadedWorkers(result2, rebalancer.getRebalancingTable()));
    Assert.assertEquals(
        "All older workers used except removed workers",
        Sets.difference(result1.usedWorkers, result2.usedWorkers),
        Sets.newHashSet(workersToBeRemoved));

    Set<Long> newWorkers = Sets.difference(result2.usedWorkers, result1.usedWorkers);
    Assert.assertTrue(
        "newly assigned workers are running",
        workers
            .values()
            .stream()
            .filter(w -> newWorkers.contains(w.getNode().getId()))
            .allMatch(w -> w.getState() == WorkerState.WORKER_STATE_WORKING));

    // jobs which were running on the workers to be removed should be updated
    Assert.assertTrue(
        jobs.values()
            .stream()
            .filter(
                jobGroup ->
                    jobGroup
                        .getJobs()
                        .values()
                        .stream()
                        .anyMatch(j -> jobsWithRemovedWorkers.contains(j.getJob().getJobId())))
            .allMatch(RebalancingJobGroup::isChanged));

    // workers were changed
    Set<Long> workerIdOnJobsWithRemovedWorkers =
        result2
            .jobToWorkerId
            .entrySet()
            .stream()
            .filter(e -> jobsWithRemovedWorkers.contains(e.getKey()))
            .map(Map.Entry::getValue)
            .collect(Collectors.toSet());
    Assert.assertTrue(Collections.disjoint(workerIdOnJobsWithRemovedWorkers, workersToBeRemoved));
  }

  @Test
  public void testJsonDataWorkloadIncreased() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    config.setWorkerToReduceRatio(0.9);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 4);

    // make sure there is spare workers
    putWorkerToMap(
        workers, LongStream.rangeClosed(1, 60).boxed().mapToLong(Long::longValue).toArray());
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);
    Set<Long> usedWorkers = usedWorkers(jobs, workers);
    Assert.assertTrue(usedWorkers.size() < workers.size());

    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup("newGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 0, "a", 5000));
    jobs.put("newGroup", rebalancingJobGroup);

    Map<Long, Long> jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));

    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);
    Map<Long, Long> prevJobToWorkerId = jobToWorkerId;
    jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));
    int diff = calcDiff(prevJobToWorkerId, jobToWorkerId);
    // at least one job got updated and other jobs can be updated due to load balance
    Assert.assertTrue(diff >= 1);
  }

  @Test
  public void testJsonDataWorkloadIncreasesThenDecreases() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    config.setWorkerToReduceRatio(0.9);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> extraWorkload1 =
        new HashMap<>(extraWorkloadOneJobGroupScaleOne);
    Map<String, RebalancingJobGroup> extraWorkload2 = new HashMap<>(extraWorkloadOneJobGroup);
    Map<String, RebalancingJobGroup> extraWorkload3 =
        new HashMap<>(extraWorkloadOneJobGroupOverloadWorkers);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);

    RebalanceSimResult result1 =
        runRebalanceSim(rebalancer::computeWorkerId, this::usedWorkers, jobs, workers, 4);
    Assert.assertTrue(validateOverloadedWorkers(result1, rebalancer.getRebalancingTable()));

    // add jobs
    jobs.putAll(extraWorkload1);
    jobs.putAll(extraWorkload2);
    jobs.putAll(extraWorkload3);

    RebalanceSimResult result2 =
        runRebalanceSim(rebalancer::computeWorkerId, this::usedWorkers, jobs, workers, 2);
    Assert.assertTrue(validateOverloadedWorkers(result2, rebalancer.getRebalancingTable()));

    // remove first set of jobs
    extraWorkload3.keySet().forEach(jobs::remove);
    RebalanceSimResult result3 =
        runRebalanceSim(rebalancer::computeWorkerId, this::usedWorkers, jobs, workers, 2);
    Assert.assertTrue(validateOverloadedWorkers(result3, rebalancer.getRebalancingTable()));

    // remove all extra jobs
    extraWorkload1.keySet().forEach(jobs::remove);
    extraWorkload2.keySet().forEach(jobs::remove);

    RebalanceSimResult result4 =
        runRebalanceSim(rebalancer::computeWorkerId, this::usedWorkers, jobs, workers, 2);
    Assert.assertTrue(validateOverloadedWorkers(result4, rebalancer.getRebalancingTable()));

    int workerCount1 = result1.usedWorkers.size();
    int workerCount2 = result2.usedWorkers.size();
    int workerCount3 = result3.usedWorkers.size();
    int workerCount4 = result4.usedWorkers.size();

    int diff1 = calcDiff(result1.jobToWorkerId, result2.jobToWorkerId);
    int diff2 = calcDiff(result2.jobToWorkerId, result3.jobToWorkerId);
    int diff3 = calcDiff(result3.jobToWorkerId, result4.jobToWorkerId);

    Assert.assertTrue("at least 6 jobs added for first diff", diff1 >= 6);
    Assert.assertTrue("Multiple workers needed to be added", workerCount2 - workerCount1 > 1);
    Assert.assertTrue(
        "Two dedicated workers should have been removed between 2 and 3",
        workerCount2 - workerCount3 >= 2);
    Assert.assertTrue(
        "Worker count should decrease by at least one", workerCount3 - workerCount4 >= 1);
    Assert.assertTrue(diff2 > 0 && diff3 > 0);
  }

  @Test
  public void testOverloadCase() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setMessagesPerSecPerWorker(4000);
    config.setWorkerToReduceRatio(0.9);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);

    RebalanceSimResult result1 =
        runRebalanceSim(rebalancer::computeWorkerId, this::usedWorkers, jobs, workers, 4);
    Assert.assertTrue(validateOverloadedWorkers(result1, rebalancer.getRebalancingTable()));
    List<Integer> overloadedWorkers1 = collectOverloadedWorkers(result1);

    // 20 workers less than total workload would need
    List<RebalancingWorkerWithSortedJobs> allWorkersWithWorkload = new ArrayList<>();
    List<Long> idleWorkers = new ArrayList<>();
    Map<Long, StoredWorker> allAssignedWorkers = new HashMap<>();
    for (int i = 0; i < config.getNumberOfVirtualPartitions(); i++) {
      List<RebalancingWorkerWithSortedJobs> workerWithinPartition =
          rebalancer.getRebalancingTable().getAllWorkersForPartition(i);
      for (RebalancingWorkerWithSortedJobs worker : workerWithinPartition) {
        if (worker.getNumberOfJobs() > 0) {
          allWorkersWithWorkload.add(worker);
        } else if (worker.getNumberOfJobs() == 0) {
          idleWorkers.add(worker.getWorkerId());
        }
        allAssignedWorkers.put(worker.getWorkerId(), workers.get(worker.getWorkerId()));
      }
    }

    Collections.sort(allWorkersWithWorkload);
    // remove the last 20 least-loaded worker to avoid shuffling jobs
    allWorkersWithWorkload = allWorkersWithWorkload.stream().limit(20).collect(Collectors.toList());
    for (RebalancingWorkerWithSortedJobs toBeRemovedWorker : allWorkersWithWorkload) {
      allAssignedWorkers.remove(toBeRemovedWorker.getWorkerId());
    }

    // remove all idle workers as well
    for (Long workerId : idleWorkers) {
      allAssignedWorkers.remove(workerId);
    }

    RebalanceSimResult result2 =
        runRebalanceSim(
            rebalancer::computeWorkerId, this::usedWorkers, jobs, allAssignedWorkers, 2);
    Assert.assertTrue(
        "remaining workers all working",
        workers
            .values()
            .stream()
            .filter(w -> result2.usedWorkers.contains(w.getNode().getId()))
            .allMatch(w -> w.getState() == WorkerState.WORKER_STATE_WORKING));
    Assert.assertTrue(result1.usedWorkers.size() - result2.usedWorkers.size() >= 20);
    Assert.assertTrue(validateOverloadedWorkers(result2, rebalancer.getRebalancingTable()));
    List<Integer> overloadedWorkers2 = collectOverloadedWorkers(result2);
    Assert.assertTrue(overloadedWorkers2.size() > overloadedWorkers1.size());
  }

  @Test
  public void testInsertWorkersIntoRebalancingTableWithInsufficentWorker() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumberOfVirtualPartitions(8);

    RpcJobColocatingRebalancer.RebalancingWorkerTable rebalancingWorkerTable =
        new RpcJobColocatingRebalancer.RebalancingWorkerTable();
    int workerIdIdx = 1000;
    for (int partitionIdx = 0; partitionIdx < 8; partitionIdx++) {
      // every partition has 4 workers to start
      for (int j = 0; j < 4; j++) {
        rebalancingWorkerTable.put(workerIdIdx++, partitionIdx);
      }
    }

    List<Integer> workersNeededPerPartition = Arrays.asList(6, 6, 4, 4, 6, 6, 8, 8);
    Map<Long, StoredWorker> workerMap = new HashMap<>();
    for (int i = 1000; i < 1042; i++) {
      workerMap.put((long) i, StoredWorker.newBuilder().build());
    }

    // only testing worker count logic
    Map<String, Integer> jobGroupToPartitionMap = new HashMap<>();
    Map<String, RebalancingJobGroup> jobGroupMap = new HashMap<>();

    RebalancerCommon.insertWorkersIntoRebalancingTable(
        rebalancingWorkerTable,
        workersNeededPerPartition,
        workerMap,
        8,
        config,
        jobGroupToPartitionMap,
        jobGroupMap);
    for (int i = 0; i < 6; i++) {
      Assert.assertEquals(
          workersNeededPerPartition.get(i).intValue(),
          rebalancingWorkerTable.getAllWorkerIdsForPartition(i).size());
    }

    Assert.assertEquals(5, rebalancingWorkerTable.getAllWorkerIdsForPartition(6).size());
    Assert.assertEquals(5, rebalancingWorkerTable.getAllWorkerIdsForPartition(7).size());
  }

  @Test
  public void testInsertWorkersIntoRebalancingTableWithSufficientWorker() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumberOfVirtualPartitions(8);

    RpcJobColocatingRebalancer.RebalancingWorkerTable rebalancingWorkerTable =
        new RpcJobColocatingRebalancer.RebalancingWorkerTable();
    List<Integer> workersInEachPartition = Arrays.asList(8, 8, 5, 5, 6, 6, 8, 8);
    int workerIdIdx = 1000;

    for (int partitionIdx = 0; partitionIdx < 8; partitionIdx++) {
      for (int j = 0; j < workersInEachPartition.get(partitionIdx); j++) {
        rebalancingWorkerTable.put(workerIdIdx++, partitionIdx);
      }
    }

    List<Integer> workersNeededPerPartition = Arrays.asList(7, 7, 8, 8, 8, 8, 8, 8);
    Map<Long, StoredWorker> workerMap = new HashMap<>();
    for (int i = 1000; i < 1064; i++) {
      workerMap.put((long) i, StoredWorker.newBuilder().build());
    }
    Map<String, Integer> jobGroupToPartitionMap = new HashMap<>();
    Map<String, RebalancingJobGroup> jobGroupMap = new HashMap<>();

    RebalancerCommon.insertWorkersIntoRebalancingTable(
        rebalancingWorkerTable,
        workersNeededPerPartition,
        workerMap,
        8,
        config,
        jobGroupToPartitionMap,
        jobGroupMap);
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(8, rebalancingWorkerTable.getAllWorkerIdsForPartition(i).size());
    }
    for (int i = 2; i < 8; i++) {
      Assert.assertEquals(
          workersNeededPerPartition.get(i).intValue(),
          rebalancingWorkerTable.getAllWorkerIdsForPartition(i).size());
    }
  }

  @Test
  public void testInsertWorkersIntoRebalancingTableRemovesFromRebalancingTable() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumberOfVirtualPartitions(8);
    config.setWorkerToReduceRatio(1.0);
    RpcJobColocatingRebalancer.RebalancingWorkerTable spyRebalancingWorkerTable =
        spy(new RpcJobColocatingRebalancer.RebalancingWorkerTable());

    List<Integer> workersInEachPartition = Arrays.asList(15, 15, 15, 15, 15, 15, 15, 15);
    int workerIdIdx = 1000;

    for (int partitionIdx = 0; partitionIdx < 8; partitionIdx++) {
      for (int j = 0; j < workersInEachPartition.get(partitionIdx); j++) {
        spyRebalancingWorkerTable.put(workerIdIdx++, partitionIdx);
      }
    }

    List<Integer> workersNeededPerPartition = Arrays.asList(7, 7, 8, 8, 8, 8, 8, 8);
    Map<Long, StoredWorker> workerMap = new HashMap<>();
    for (int i = 1000;
        i < workersInEachPartition.stream().mapToInt(Integer::intValue).sum() + 1000;
        i++) {
      workerMap.put((long) i, StoredWorker.newBuilder().build());
    }
    Map<String, Integer> jobGroupToPartitionMap = new HashMap<>();
    Map<String, RebalancingJobGroup> jobGroupMap = new HashMap<>();

    for (int i = 0; i < workerMap.size(); i++) {
      jobGroupToPartitionMap.put("job" + i, (1 + i) % 8);
    }
    List<Long> workerIds = new ArrayList<>(workerMap.keySet());
    for (int i = 0; i < workerMap.size(); i++) {
      jobGroupMap.put(
          "job" + i,
          buildRebalancingJobGroup(
              "job" + i,
              JobState.JOB_STATE_RUNNING,
              StoredJob.newBuilder().setWorkerId(workerIds.get(i)).build()));
    }

    // test logic to not put worker in same partition: find job group in partition 0, put same
    // worker in virtual partition 1
    Map.Entry<String, Integer> e =
        jobGroupToPartitionMap
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue() == 0)
            .findFirst()
            .get();
    Long workerIdInDuplicatePartitions =
        jobGroupMap.get(e.getKey()).getJobs().values().stream().findFirst().get().getWorkerId();
    jobGroupMap.put(
        "customJob",
        buildRebalancingJobGroup(
            "customJob",
            JobState.JOB_STATE_RUNNING,
            StoredJob.newBuilder().setWorkerId(workerIdInDuplicatePartitions).build()));
    jobGroupToPartitionMap.put("customJob", 1);

    RebalancerCommon.insertWorkersIntoRebalancingTable(
        spyRebalancingWorkerTable,
        workersNeededPerPartition,
        workerMap,
        8,
        config,
        jobGroupToPartitionMap,
        jobGroupMap);

    // worker should only have been put in one partition
    Mockito.verify(spyRebalancingWorkerTable, Mockito.times(1))
        .put(eq(workerIdInDuplicatePartitions), anyLong());
    int numOriginalWorkers = workersInEachPartition.stream().mapToInt(Integer::intValue).sum();
    int numWorkersNeeded = workersNeededPerPartition.stream().mapToInt(Integer::intValue).sum();
    Mockito.verify(spyRebalancingWorkerTable, Mockito.times(numOriginalWorkers - numWorkersNeeded))
        .removeWorker(anyLong());
    Mockito.verify(
            spyRebalancingWorkerTable,
            Mockito.times(workersInEachPartition.stream().mapToInt(Integer::intValue).sum() + 1))
        .putIfAbsent(anyLong(), anyLong());
    for (int i = 0; i < 8; i++) {
      Assert.assertEquals(
          workersNeededPerPartition.get(i).intValue(),
          spyRebalancingWorkerTable.getAllWorkerIdsForPartition(i).size());
    }
  }

  @Test
  public void testJsonDataHaveWorkerInDifferentPartititon() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    config.setWorkerToReduceRatio(0.9);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);

    RebalanceSimResult result1 =
        runRebalanceSim(rebalancer::computeWorkerId, this::usedWorkers, jobs, workers, 4);
    Assert.assertTrue(validateOverloadedWorkers(result1, rebalancer.getRebalancingTable()));

    long workerInPartition0 =
        rebalancer
            .getRebalancingTable()
            .getAllWorkersForPartition(0)
            .stream()
            .findFirst()
            .get()
            .getWorkerId();
    long workerInPartiton1 =
        rebalancer
            .getRebalancingTable()
            .getAllWorkersForPartition(1)
            .stream()
            .findFirst()
            .get()
            .getWorkerId();

    RebalancingJobGroup jobGroupInPartition1 =
        jobs.values()
            .stream()
            .filter(
                j ->
                    j.getJobs()
                        .values()
                        .stream()
                        .anyMatch(j1 -> j1.getWorkerId() == workerInPartiton1))
            .findFirst()
            .get();
    long targetedJob = jobGroupInPartition1.getJobs().keySet().stream().findFirst().get();
    jobGroupInPartition1.updateJob(
        targetedJob,
        StoredJob.newBuilder(jobGroupInPartition1.getJobs().get(targetedJob))
            .setWorkerId(workerInPartition0)
            .build());

    jobs.put(jobGroupInPartition1.getJobGroup().getJobGroupId(), jobGroupInPartition1);

    RebalanceSimResult result2 =
        runRebalanceSim(rebalancer::computeWorkerId, this::usedWorkers, jobs, workers, 2);
    boolean isWorkerInPartition0 =
        rebalancer
            .getRebalancingTable()
            .getAllWorkersForPartition(0)
            .stream()
            .anyMatch(w -> w.getWorkerId() == workerInPartition0);
    boolean isWorkerInPartition1 =
        rebalancer
            .getRebalancingTable()
            .getAllWorkersForPartition(1)
            .stream()
            .anyMatch(w -> w.getWorkerId() == workerInPartition0);

    Assert.assertTrue(isWorkerInPartition0 ^ isWorkerInPartition1);
    Assert.assertTrue(validateOverloadedWorkers(result2, rebalancer.getRebalancingTable()));
    Assert.assertTrue(calcDiff(result1.jobToWorkerId, result2.jobToWorkerId) > 0);
  }

  @Test
  public void testMoveJobsToIdleWorkersWhenWorkersAreOverloadedWithMultipleLargeJobs()
      throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    config.setWorkerToReduceRatio(0.9);
    config.setTargetSpareWorkerPercentage(50);
    config.setNumberOfVirtualPartitions(8);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);
    Set<Long> usedWorkers = usedWorkers(workerOverloadedCaseJobs, workerOverloadedCaseWorkers);
    Set<Long> idleWorkerIds = new HashSet<>(workerOverloadedCaseWorkers.keySet());
    idleWorkerIds.removeAll(usedWorkers);

    RebalanceSimResult result1 =
        runRebalanceSim(
            rebalancer::computeWorkerId,
            this::usedWorkers,
            workerOverloadedCaseJobs,
            workerOverloadedCaseWorkers,
            2);
    Set<Long> newIdleWorkerIds = new HashSet<>(workerOverloadedCaseWorkers.keySet());
    newIdleWorkerIds.removeAll(result1.usedWorkers);
    Assert.assertTrue(idleWorkerIds.size() > newIdleWorkerIds.size());
  }

  @Override
  Set<Long> usedWorkers(Map<String, RebalancingJobGroup> jobs, Map<Long, StoredWorker> workers) {
    Set<Long> usedWorkers = new HashSet<>();
    for (Map.Entry<String, RebalancingJobGroup> entry : jobs.entrySet()) {
      for (Map.Entry<Long, StoredJob> job : entry.getValue().getJobs().entrySet()) {
        usedWorkers.add(job.getValue().getWorkerId());
      }
    }
    return usedWorkers;
  }

  private static ImmutableMap<Long, Long> jobToWorkerId(Collection<StoredJob> jobs) {
    return ImmutableMap.copyOf(
        jobs.stream().collect(Collectors.toMap(e -> e.getJob().getJobId(), e -> e.getWorkerId())));
  }

  private static Map<String, RebalancingJobGroup> readJsonJobs(Reader reader) throws IOException {
    DebugJobsTable.Builder tableBuilder = DebugJobsTable.newBuilder();
    JsonFormat.parser().merge(reader, tableBuilder);
    DebugJobsTable jobsTable = tableBuilder.build();
    Map<String, StoredJobGroup.Builder> builderMap = new HashMap<>();
    double scale = 0.0;
    for (DebugJobRow row : jobsTable.getDataList()) {
      builderMap
          .computeIfAbsent(row.getJobGroupId(), o -> StoredJobGroup.newBuilder())
          .addJobs(row.getJob());
      scale += row.getJob().getScale();
    }
    ScaleStatus scaleStatus = ScaleStatus.newBuilder().setScale(scale).build();
    return builderMap
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    RebalancingJobGroup.of(
                        Versioned.from(
                            e.getValue()
                                .setState(JobState.JOB_STATE_RUNNING)
                                .setJobGroup(JobGroup.newBuilder().setJobGroupId(e.getKey()))
                                .setScaleStatus(scaleStatus)
                                .build(),
                            1),
                        ImmutableMap.of())));
  }

  private static List<Integer> collectOverloadedWorkers(RebalanceSimResult rebalanceSimResult) {
    // Worker should either have a single Job > 1.0 scale, or multiple jobs with total scale < 1.0
    // scale in happy path
    List<Integer> overloadedWorkers = new ArrayList<>();
    Map<Long, Double> workerIdToWorkload = new HashMap<>();
    for (StoredJob job :
        rebalanceSimResult
            .jobGroupMap
            .values()
            .stream()
            .map(RebalancingJobGroup::getJobs)
            .flatMap(m -> m.values().stream())
            .collect(Collectors.toList())) {
      Long workerId = job.getWorkerId();
      Double scale = job.getScale();
      if (scale > 1.0) {
        if (workerIdToWorkload.get(workerId) != null) {
          overloadedWorkers.add(workerId.intValue());
        }
        workerIdToWorkload.put(workerId, scale);
      } else {
        workerIdToWorkload.put(
            workerId,
            workerIdToWorkload.get(workerId) == null
                ? scale
                : workerIdToWorkload.get(workerId) + scale);
        if (workerIdToWorkload.get(workerId) > 1.0) {
          overloadedWorkers.add(workerId.intValue());
        }
      }
    }
    return overloadedWorkers;
  }

  /* If worker is overloaded, make sure no other workers in the same partition could help shed the load */
  private static boolean validateOverloadedWorkers(
      RebalanceSimResult rebalanceSimResult,
      RpcJobColocatingRebalancer.RebalancingWorkerTable rebalancingWorkerTable) {
    List<Integer> overloadedWorkers = collectOverloadedWorkers(rebalanceSimResult);
    List<Long> partitions = rebalancingWorkerTable.getAllPartitions();
    Map<Long, Long> workerToPartition = new HashMap<>();
    for (Long partition : partitions) {
      List<Long> workers =
          rebalancingWorkerTable
              .getAllWorkersForPartition(partition)
              .stream()
              .map(RebalancingWorkerWithSortedJobs::getWorkerId)
              .collect(Collectors.toList());
      workers.forEach(worker -> workerToPartition.put(worker, partition));
    }
    for (int workerId : overloadedWorkers) {
      List<RebalancingWorkerWithSortedJobs> otherWorkers =
          rebalancingWorkerTable.getAllWorkersForPartition(
              workerToPartition.get(Long.valueOf(workerId)));
      RebalancingWorkerWithSortedJobs targetWorker =
          otherWorkers.stream().filter(w -> w.getWorkerId() == workerId).findFirst().get();
      List<RebalancingJob> targetJobs = targetWorker.getAllJobs();
      for (RebalancingWorkerWithSortedJobs otherWorker : otherWorkers) {
        if (otherWorker.getWorkerId() != workerId) {
          Double otherWorkerLoad = otherWorker.getLoad();
          for (RebalancingJob targetJob : targetJobs) {
            if (otherWorkerLoad + targetJob.getLoad() <= 1.0) {
              return false;
            }
          }
        }
      }
    }
    return true;
  }

  private static Map<Long, StoredWorker> putWorkerToMap(
      Map<Long, StoredWorker> workerMap, long... workerIds) {
    for (long workerId : workerIds) {
      StoredWorker.Builder workerBuilder = StoredWorker.newBuilder();
      workerBuilder.getNodeBuilder().setId(workerId);
      workerBuilder.getNodeBuilder().setHost("dca11-" + workerId);
      workerBuilder.getNodeBuilder().setPort((int) (9000 + workerId));
      workerMap.put(workerId, workerBuilder.build());
    }
    return workerMap;
  }

  private static <T, G> ImmutableMap<T, G> mergeMaps(List<Map<T, G>> maps) {
    return ImmutableMap.copyOf(
        (maps.stream()
            .flatMap(m -> m.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
  }

  // consolidated method which runs the rebalance and returns useful results
  protected RebalanceSimResult runRebalanceSim(
      BiConsumer<Map<String, RebalancingJobGroup>, Map<Long, StoredWorker>> rebalanceFunction,
      BiFunction<Map<String, RebalancingJobGroup>, Map<Long, StoredWorker>, Set<Long>>
          usedWorkersFunction,
      Map<String, RebalancingJobGroup> jobs,
      Map<Long, StoredWorker> workers,
      int maxRound) {
    RebalanceSimResult result = new RebalanceSimResult();
    result.round = runRebalanceToConverge(rebalanceFunction, jobs, workers, maxRound);
    result.usedWorkers = usedWorkersFunction.apply(jobs, workers);
    result.jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));
    result.jobGroupMap = jobs;
    result.workers = workers;
    return result;
  }

  private static class RebalanceSimResult {
    public int round;
    public Set<Long> usedWorkers;
    public Map<Long, Long> jobToWorkerId;
    public Map<String, RebalancingJobGroup> jobGroupMap;
    public Map<Long, StoredWorker> workers;
  }
}
