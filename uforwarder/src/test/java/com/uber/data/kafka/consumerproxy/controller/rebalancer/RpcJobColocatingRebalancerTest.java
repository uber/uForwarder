package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
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
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
          {"data/jobs.json", "data/workers.json"},
          {"data/tier5jobs.json", "data/tier5workers.json"}
        });
  }

  private ImmutableMap<String, RebalancingJobGroup> jsonJobs;
  private ImmutableMap<Long, StoredWorker> jsonWorkers;

  private Scope mockScope;

  private Scope mockSubScope;

  private Gauge mockGauge;

  private Counter mockCounter;

  private Scalar scalar;

  private HibernatingJobRebalancer hibernatingJobRebalancer;

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

    // To load production data for test, download data from
    // https://system-phx.uberinternal.com/udg://kafka-consumer-proxy-master-phx/0:system/workersJson
    jsonWorkers =
        ImmutableMap.copyOf(
            readJsonWorkers(
                new InputStreamReader(
                    this.getClass().getClassLoader().getResourceAsStream(workerDataPath))));
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

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);

    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    int round = runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);
    Set<Long> usedWorkers = usedWorkers(jobs, workers);
    workers.keySet().removeAll(usedWorkers);
    Assert.assertFalse(usedWorkers.contains(0L));
    Assert.assertTrue(round < 2);
  }

  @Override
  @Test
  public void testJsonDataRemoveJobGroup() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setMessagesPerSecPerWorker(4000);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);

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
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);

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
  }

  @Override
  @Test
  public void testJsonDataRemoveWorker() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);

    Map<Long, Long> jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));

    Long removedWorkerId = workers.keySet().stream().findAny().get();
    workers.remove(removedWorkerId);
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
    Assert.assertTrue(
        jobIds.stream().anyMatch(id -> prevJobToWorkerId.get(id).equals(removedWorkerId)));
  }

  @Override
  @Test
  public void testJsonDataAddJobGroup() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    RpcJobColocatingRebalancer rebalancer =
        new RpcJobColocatingRebalancer(mockScope, config, scalar, hibernatingJobRebalancer, true);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);

    // make sure there is spare workers
    putWorkerToMap(
        workers, LongStream.rangeClosed(1, 60).boxed().mapToLong(Long::longValue).toArray());
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);
    Set<Long> usedWorkers = usedWorkers(jobs, workers);
    Assert.assertTrue(usedWorkers.size() < workers.size());

    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(1, 0, "a", 12));
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
}
