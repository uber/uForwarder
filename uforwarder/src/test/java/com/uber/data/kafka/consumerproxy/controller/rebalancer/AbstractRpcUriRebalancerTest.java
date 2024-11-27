package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import static com.uber.data.kafka.consumerproxy.controller.rebalancer.RebalancerCommon.roundUpToNearestNumber;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.DebugJobRow;
import com.uber.data.kafka.datatransfer.DebugJobsTable;
import com.uber.data.kafka.datatransfer.DebugWorkerRow;
import com.uber.data.kafka.datatransfer.DebugWorkersTable;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.ScaleStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AbstractRpcUriRebalancerTest extends FievelTestBase {

  AbstractRpcUriRebalancer rpcUriJobAssigner;
  static int messagesPerSecPerWorker = 10;
  Scope mockScope;
  Gauge mockSpareWorkerGauge;
  Gauge mockTargetWorkerGauge;
  StoredJob jobOne;
  StoredJob jobTwo;
  StoredJob jobThree;
  Map<Long, StoredWorker> workerMap;
  ImmutableMap<String, RebalancingJobGroup> jsonJobs;
  ImmutableMap<Long, StoredWorker> jsonWorkers;
  Scalar scalar;
  HibernatingJobRebalancer hibernatingJobRebalancer;

  // suppress ForbidClassloaderGetResourceInTests as moving to gradle repo
  @SuppressWarnings("ForbidClassloaderGetResourceInTests")
  @Before
  public void setup() throws IOException {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(messagesPerSecPerWorker);
    config.setTargetSpareWorkerPercentage(151);
    config.setMinimumWorkerCount(6);
    mockScope = Mockito.mock(Scope.class);
    mockSpareWorkerGauge = Mockito.mock(Gauge.class);
    mockTargetWorkerGauge = Mockito.mock(Gauge.class);
    scalar = Mockito.mock(Scalar.class);
    hibernatingJobRebalancer = Mockito.mock(HibernatingJobRebalancer.class);
    Mockito.doAnswer(
            invocation -> {
              RebalancingJobGroup rebalancingJobGroup = invocation.getArgument(0);
              double defualtScale = invocation.getArgument(1);
              rebalancingJobGroup.updateScale(defualtScale);
              return null;
            })
        .when(scalar)
        .apply(Mockito.any(RebalancingJobGroup.class), Mockito.anyDouble());
    Mockito.doAnswer(
            invocation -> {
              return Collections.EMPTY_SET;
            })
        .when(hibernatingJobRebalancer)
        .computeWorkerId(Mockito.anyList(), Mockito.anyMap());
    Mockito.when(mockScope.tagged(Mockito.anyMap())).thenReturn(mockScope);
    Mockito.when(mockScope.gauge(Mockito.anyString())).thenReturn(Mockito.mock(Gauge.class));
    Mockito.when(mockScope.counter(Mockito.anyString())).thenReturn(Mockito.mock(Counter.class));
    Mockito.when(mockScope.gauge(AbstractRpcUriRebalancer.MetricNames.WORKERS_SPARE))
        .thenReturn(mockSpareWorkerGauge);
    Mockito.when(mockScope.gauge(AbstractRpcUriRebalancer.MetricNames.WORKERS_TARGET))
        .thenReturn(mockTargetWorkerGauge);
    rpcUriJobAssigner = new SimpleRpcUriAssigner(mockScope, config, scalar);

    StoredJob.Builder jobBuilder = StoredJob.newBuilder();

    jobBuilder.getJobBuilder().setJobId(1L);
    jobBuilder.setWorkerId(1L);
    jobOne = jobBuilder.build();

    jobBuilder.clear();
    jobBuilder.getJobBuilder().setJobId(2L);
    jobBuilder.setWorkerId(2L);
    jobTwo = jobBuilder.build();

    jobBuilder.clear();
    jobBuilder.getJobBuilder().setJobId(3L);
    jobThree = jobBuilder.build();

    StoredWorker.Builder workerBuilder = StoredWorker.newBuilder();

    workerBuilder.getNodeBuilder().setId(2);
    StoredWorker workerTwo = workerBuilder.build();

    workerBuilder.getNodeBuilder().setId(3);
    StoredWorker workerThree = workerBuilder.build();

    workerMap =
        ImmutableMap.of(
            2L, workerTwo,
            3L, workerThree);

    // To load production data for test, download data from
    // https://system-phx.uberinternal.com/udg://kafka-consumer-proxy-master-phx/0:system/jobsJson
    jsonJobs =
        ImmutableMap.copyOf(
            readJsonJobs(
                new InputStreamReader(
                    this.getClass().getClassLoader().getResourceAsStream("data/jobs.json"))));

    // To load production data for test, download data from
    // https://system-phx.uberinternal.com/udg://kafka-consumer-proxy-master-phx/0:system/workersJson
    jsonWorkers =
        ImmutableMap.copyOf(
            readJsonWorkers(
                new InputStreamReader(
                    this.getClass().getClassLoader().getResourceAsStream("data/workers.json"))));
  }

  static StoredJob buildJob(long jobId, long workerId) {
    return buildJob(jobId, workerId, "", 6);
  }

  static StoredJob buildJob(long jobId, long workerId, String uri, double rps) {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    jobBuilder.getJobBuilder().setJobId(jobId);
    jobBuilder.getJobBuilder().getFlowControlBuilder().setMessagesPerSec(rps);
    jobBuilder.setWorkerId(workerId);
    jobBuilder.getJobBuilder().getRpcDispatcherTaskBuilder().setUri(uri);
    jobBuilder.setScale(rps / messagesPerSecPerWorker);
    return jobBuilder.build();
  }

  private void updateScale(RebalancingJobGroup rebalancingJobGroup) {
    for (Map.Entry<Long, StoredJob> entry : rebalancingJobGroup.getJobs().entrySet()) {
      rebalancingJobGroup.updateJob(
          entry.getKey(),
          StoredJob.newBuilder(entry.getValue())
              .setScale(
                  entry.getValue().getJob().getFlowControl().getMessagesPerSec()
                      / rpcUriJobAssigner.config.getMessagesPerSecPerWorker())
              .build());
    }
  }

  RebalancingJobGroup buildRebalancingJobGroup(
      String jobGroupId, JobState jobGroupState, StoredJob... jobs) {
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    double mps = 0;
    for (StoredJob job : jobs) {
      jobGroupBuilder.addJobs(job.toBuilder().setState(jobGroupState).build());
      mps += job.getJob().getFlowControl().getMessagesPerSec();
    }
    StoredJobGroup jobGroup =
        jobGroupBuilder
            .setJobGroup(
                JobGroup.newBuilder()
                    .setFlowControl(FlowControl.newBuilder().setMessagesPerSec(mps).build())
                    .setJobGroupId(jobGroupId)
                    .buildPartial())
            .setState(jobGroupState)
            .build();
    RebalancingJobGroup result =
        RebalancingJobGroup.of(Versioned.from(jobGroup, 1), ImmutableMap.of());
    return RebalancingJobGroup.of(result.toStoredJobGroup(), ImmutableMap.of());
  }

  RebalancingJobGroup buildRebalancingJobGroup(
      String jobGroupId,
      JobState jobGroupState,
      double mps,
      double bps,
      double inflight,
      double scale,
      StoredJob... jobs) {
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    for (StoredJob job : jobs) {
      jobGroupBuilder.addJobs(job.toBuilder().setState(jobGroupState).build());
    }
    StoredJobGroup jobGroup =
        jobGroupBuilder
            .setJobGroup(
                JobGroup.newBuilder()
                    .setFlowControl(
                        FlowControl.newBuilder()
                            .setMessagesPerSec(mps)
                            .setBytesPerSec(bps)
                            .setMaxInflightMessages(inflight)
                            .build())
                    .setJobGroupId(jobGroupId)
                    .buildPartial())
            .setState(jobGroupState)
            .setScaleStatus(ScaleStatus.newBuilder().setScale(scale).build())
            .build();
    RebalancingJobGroup result =
        RebalancingJobGroup.of(Versioned.from(jobGroup, 1), ImmutableMap.of());
    return RebalancingJobGroup.of(result.toStoredJobGroup(), ImmutableMap.of());
  }

  private static Map<Long, StoredWorker> buildWorkerMap(long... workerIds) {
    return putWorkerToMap(new HashMap<>(), workerIds);
  }

  private static Map<Long, StoredWorker> putWorkerToMap(
      Map<Long, StoredWorker> workerMap, long... workerIds) {
    for (long workerId : workerIds) {
      StoredWorker.Builder workerBuilder = StoredWorker.newBuilder();
      workerBuilder.getNodeBuilder().setId(workerId);
      workerMap.put(workerId, workerBuilder.build());
    }
    return workerMap;
  }

  private static Set<Long> getWorkersIds(Map<Long, StoredJob> jobMap) {
    return jobMap.values().stream().map(j -> j.getWorkerId()).collect(Collectors.toSet());
  }

  private static Double getMaxLoadOfWorker(Map<Long, StoredJob> jobMap) {
    return jobMap
        .values()
        .stream()
        .collect(
            Collectors.groupingBy(
                e -> e.getWorkerId(),
                Collectors.summingDouble(e -> e.getJob().getFlowControl().getMessagesPerSec())))
        .values()
        .stream()
        .max(Double::compareTo)
        .get();
  }

  private static ImmutableMap<Long, Long> jobToWorkerId(Collection<StoredJob> jobs) {
    return ImmutableMap.copyOf(
        jobs.stream().collect(Collectors.toMap(e -> e.getJob().getJobId(), e -> e.getWorkerId())));
  }

  static Set<Long> updatedJob(Map<Long, Long> src, Map<Long, Long> dest) {
    Set<Map.Entry<Long, Long>> srcSet = new HashSet<>(src.entrySet());
    Set<Map.Entry<Long, Long>> destSet = new HashSet<>(dest.entrySet());
    destSet.removeAll(srcSet);
    return destSet.stream().map(o -> o.getKey()).collect(Collectors.toSet());
  }

  static Set<Long> deletedJob(Map<Long, Long> src, Map<Long, Long> dest) {
    Set<Long> deleted = new HashSet<>(src.keySet());
    deleted.removeAll(dest.keySet());
    return deleted;
  }

  static int calcDiff(Map<Long, Long> src, Map<Long, Long> dest) {
    // diff equals changed mapping + deleted mapping
    return updatedJob(src, dest).size() + deletedJob(src, dest).size();
  }

  private static Map<String, RebalancingJobGroup> readJsonJobs(Reader reader) throws IOException {
    DebugJobsTable.Builder tableBuilder = DebugJobsTable.newBuilder();
    JsonFormat.parser().merge(reader, tableBuilder);
    DebugJobsTable jobsTable = tableBuilder.build();
    Map<String, StoredJobGroup.Builder> builderMap = new HashMap<>();
    for (DebugJobRow row : jobsTable.getDataList()) {
      builderMap
          .computeIfAbsent(row.getJobGroupId(), o -> StoredJobGroup.newBuilder())
          .addJobs(row.getJob());
    }
    return builderMap
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    RebalancingJobGroup.of(
                        Versioned.from(
                            e.getValue().setState(JobState.JOB_STATE_RUNNING).build(), 1),
                        ImmutableMap.of())));
  }

  static Map<Long, StoredWorker> readJsonWorkers(Reader reader) throws IOException {
    DebugWorkersTable.Builder tableBuilder = DebugWorkersTable.newBuilder();
    JsonFormat.parser().merge(reader, tableBuilder);
    DebugWorkersTable workersTable = tableBuilder.build();
    Map<Long, StoredWorker> result = new HashMap<>();
    for (DebugWorkerRow row : workersTable.getDataList()) {
      StoredWorker worker = row.getWorker();
      result.put(worker.getNode().getId(), worker);
    }
    return result;
  }

  static class WorkerLoad {
    double rps = 0.0;
    Set<StoredJob> jobs = new HashSet<>();
    Set<String> uris = new HashSet<>();

    void add(StoredJob job) {
      rps += job.getJob().getFlowControl().getMessagesPerSec();
      jobs.add(job);
      uris.add(job.getJob().getRpcDispatcherTask().getUri());
      if (uris.size() > 1) {
        throw new IllegalStateException();
      }
    }
  }

  private static Map<String, Map<Long, WorkerLoad>> uriLoadPerWorker(
      Map<String, RebalancingJobGroup> jobs, Map<Long, StoredWorker> workers) {
    Map<String, Map<Long, WorkerLoad>> result = new HashMap<>();
    for (RebalancingJobGroup jobGroup : jobs.values()) {
      for (StoredJob job : jobGroup.getJobs().values()) {
        String uri = job.getJob().getRpcDispatcherTask().getUri();
        long workerId = job.getWorkerId();
        result
            .computeIfAbsent(uri, o -> new HashMap<>())
            .computeIfAbsent(workerId, o -> new WorkerLoad())
            .add(job);
      }
    }
    return result;
  }

  Set<Long> usedWorkers(Map<String, RebalancingJobGroup> jobs, Map<Long, StoredWorker> workers) {
    Map<String, Map<Long, WorkerLoad>> loadPerWorker = uriLoadPerWorker(jobs, workers);
    Set<Long> usedWorkers = new HashSet<>();
    for (Map<Long, WorkerLoad> map : loadPerWorker.values()) {
      for (Map.Entry<Long, WorkerLoad> entry : map.entrySet()) {
        if (entry.getValue().rps == 0.0) continue;
        if (usedWorkers.contains(entry.getKey())) {
          // worker must bound with uri
          throw new IllegalStateException();
        }
        usedWorkers.add(entry.getKey());
      }
    }
    return usedWorkers;
  }

  // return mapping from uri to worker and job list
  private Map<String, List<WorkerLoad>> uriWorkers(
      Map<String, RebalancingJobGroup> jobs, Map<Long, StoredWorker> workers) {
    Map<String, Map<Long, WorkerLoad>> loadPerWorker = uriLoadPerWorker(jobs, workers);
    Map<String, List<WorkerLoad>> uriWorkers = new HashMap<>();
    for (Map.Entry<String, Map<Long, WorkerLoad>> entry : loadPerWorker.entrySet()) {
      List<WorkerLoad> list = new ArrayList<>();
      list.addAll(entry.getValue().values());
      list.sort(Comparator.comparingDouble(o -> o.rps));
      uriWorkers.put(entry.getKey(), list);
    }

    return uriWorkers;
  }

  // returns last converged round
  int runRebalanceToConverge(
      BiConsumer<Map<String, RebalancingJobGroup>, Map<Long, StoredWorker>> rebalanceFunction,
      Map<String, RebalancingJobGroup> jobs,
      Map<Long, StoredWorker> workers,
      int maxRound) {
    int result = Integer.MAX_VALUE;
    for (int i = 0; i < maxRound; ++i) {
      Map<Long, Long> jobToWorkerId =
          jobToWorkerId(
              jobs.values()
                  .stream()
                  .flatMap(s -> s.getJobs().values().stream())
                  .collect(Collectors.toList()));
      rebalanceFunction.accept(jobs, workers);
      int diff =
          calcDiff(
              jobToWorkerId,
              jobToWorkerId(
                  jobs.values()
                      .stream()
                      .flatMap(s -> s.getJobs().values().stream())
                      .collect(Collectors.toList())));
      Set<Long> usedWorkers = usedWorkers(jobs, workers);
      Assert.assertFalse(usedWorkers.contains(0L));
      if (diff == 0) {
        if (result == Integer.MAX_VALUE) {
          result = i;
        }
      } else {
        result = Integer.MAX_VALUE;
      }
    }

    Assert.assertNotEquals(Integer.MAX_VALUE, result);
    return result;
  }

  @Test
  public void testJsonDataResultConvergeRate() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    AbstractRpcUriRebalancer rebalancer = new SimpleRpcUriAssigner(new NoopScope(), config, scalar);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);

    int round = runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 10);
    Set<Long> usedWorkers = usedWorkers(jobs, workers);
    Assert.assertFalse(usedWorkers.contains(0L));
    // result must converge in maximsum 2 rounds
    // result doesn't diverge in 10 rounds
    Assert.assertTrue(round < 2);
  }

  @Test
  public void testJsonDataRemoveJobGroup() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    AbstractRpcUriRebalancer rebalancer = new SimpleRpcUriAssigner(new NoopScope(), config, scalar);

    Map<String, RebalancingJobGroup> jobs = new HashMap<>(jsonJobs);
    Map<Long, StoredWorker> workers = new HashMap<>(jsonWorkers);
    runRebalanceToConverge(rebalancer::computeWorkerId, jobs, workers, 2);

    Map<Long, Long> jobToWorkerId =
        jobToWorkerId(
            jobs.values()
                .stream()
                .flatMap(s -> s.getJobs().values().stream())
                .collect(Collectors.toList()));

    String groupName = jobs.keySet().stream().findAny().get();
    RebalancingJobGroup group = jobs.get(groupName);
    String uri =
        group.getJobs().values().stream().findAny().get().getJob().getRpcDispatcherTask().getUri();
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

    // updated jobs and deleted job should have same URI
    Set<Long> updatedJobIds = updatedJob(prevJobToWorkerId, jobToWorkerId);
    List<StoredJob> updatedJobs =
        jobs.entrySet()
            .stream()
            .flatMap(o -> o.getValue().getJobs().values().stream())
            .filter(o -> updatedJobIds.contains(o.getJob().getJobId()))
            .collect(Collectors.toList());
    for (StoredJob updatedJob : updatedJobs) {
      Assert.assertEquals(uri, updatedJob.getJob().getRpcDispatcherTask().getUri());
    }
  }

  @Test
  public void testJsonDataAddWorker() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    AbstractRpcUriRebalancer rebalancer = new SimpleRpcUriAssigner(new NoopScope(), config, scalar);

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
    Set<Long> jobIds = updatedJob(prevJobToWorkerId, jobToWorkerId);
    for (long jobId : jobIds) {
      Assert.assertEquals(newWorkerId, jobToWorkerId.get(jobId));
    }
  }

  @Test
  public void testJsonDataRemoveWorker() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    AbstractRpcUriRebalancer rebalancer = new SimpleRpcUriAssigner(new NoopScope(), config, scalar);

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
    for (long jobId : jobIds) {
      Assert.assertEquals(removedWorkerId, prevJobToWorkerId.get(jobId));
    }
  }

  @Test
  public void testJsonDataAddJobGroup() throws Exception {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    AbstractRpcUriRebalancer rebalancer = new SimpleRpcUriAssigner(new NoopScope(), config, scalar);

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
        buildRebalancingJobGroup("newGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 0, "a", 12));
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
    // make sure only updated job get reassigned
    Assert.assertEquals(1, diff);
  }

  @Test
  public void testComputeWorkerIdNoChangeIfJobGroupCanceled() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_CANCELED, buildJob(1, 0), buildJob(2, 0));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertFalse(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(0L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerIdNoChangeIfJobGroupInvalid() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_INVALID, buildJob(1, 0), buildJob(2, 0));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertFalse(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(0L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerIdNoChangeIfJobGroupUnimplemented() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_UNIMPLEMENTED, buildJob(1, 0), buildJob(2, 0));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertFalse(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(0L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerIdNoChangeIfJobGroupFailed() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_FAILED, buildJob(1, 0), buildJob(2, 0));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertFalse(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(0L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerIdNoChangeIfNoWorkers() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 0), buildJob(2, 0));
    Map<Long, StoredWorker> workerMap = buildWorkerMap();
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertFalse(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(0L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerIdNoChangeIfStable() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 2));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertFalse(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(1L, 2L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerRebalanceBothJobsToNewWorkers() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 2));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(3, 4);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(3L, 4L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerUnassignWorkerIds() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 2));
    Map<Long, StoredWorker> workerMap = buildWorkerMap();
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(0L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerPartialRebalance() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 2));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(2, 3);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(2L, 3L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeLoad() {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 2));
    rpcUriJobAssigner.computeLoad(ImmutableMap.of("jobGroup", rebalancingJobGroup));
    Assert.assertEquals(1.2, rebalancingJobGroup.getScale().get(), 0.0001);
  }

  @Test
  public void testComputeWorkerForUnassignedJob() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 0), buildJob(2, 2));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(1L, 2L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  // TODO: There is a todo in the code for this feature. Leaving commented test for TDD.
  //  @Test
  //  public void testComputeWorkerForUnbalancedJobs() throws Exception {
  //    RebalancingJobGroup rebalancingJobGroup =
  //        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 1));
  //    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
  //    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup),
  // workerMap);
  //    Assert.assertTrue(rebalancingJobGroup.isChanged());
  //    Assert.assertEquals(ImmutableSet.of(1L, 2L), getWorkersIds(rebalancingJobGroup.getJobs()));
  //  }

  @Test
  public void testComputeWorkerIdMergeWorkersIfPossible() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 2), buildJob(3, 3));
    rpcUriJobAssigner.config.setMessagesPerSecPerWorker(15);
    Map<String, RebalancingJobGroup> jobGroupMap = ImmutableMap.of("jobGroup", rebalancingJobGroup);
    updateScale(rebalancingJobGroup);
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2, 3);
    rpcUriJobAssigner.computeWorkerId(jobGroupMap, workerMap);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    Assert.assertEquals(2, getWorkersIds(rebalancingJobGroup.getJobs()).size());
  }

  @Test
  public void testComputeWorkerIdSelectsLargestWorkerId() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 0), buildJob(2, 1));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2, 3);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(1L, 3L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerIdUsesThreeWorkersIfCapacitySpillover() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 0), buildJob(2, 0), buildJob(3, 0));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2, 3);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(1L, 2L, 3L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerIdAssignsLargeJobToOneWorkerEvenIfOverCapacity() throws Exception {
    StoredJob.Builder jobBuilder = buildJob(1, 0).toBuilder();
    jobBuilder.getJobBuilder().getFlowControlBuilder().setMessagesPerSec(20);
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup("jobGroup", JobState.JOB_STATE_RUNNING, jobBuilder.build());
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(1L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testComputeWorkerIdAssignsLargeJobMixSmallJob() {
    RebalancingJobGroup rebalancingJobGroup1 =
        buildRebalancingJobGroup("jobGroup1", JobState.JOB_STATE_RUNNING, buildJob(1, 3, "a", 12));
    RebalancingJobGroup rebalancingJobGroup2 =
        buildRebalancingJobGroup(
            "jobGroup2",
            JobState.JOB_STATE_RUNNING,
            buildJob(2, 0, "b", 6),
            buildJob(3, 0, "b", 6));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2, 3, 4);
    rpcUriJobAssigner.computeWorkerId(
        ImmutableMap.of("jobGroup1", rebalancingJobGroup1, "jobGroup2", rebalancingJobGroup2),
        workerMap);
    Assert.assertEquals(ImmutableSet.of(3L), getWorkersIds(rebalancingJobGroup1.getJobs()));
    Assert.assertEquals(ImmutableSet.of(1L, 2L), getWorkersIds(rebalancingJobGroup2.getJobs()));
  }

  @Test
  public void testComputeWorkerIdAssignsLargeJobStableAssignment() {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup("jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 0, "a", 12));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2, 3, 4, 5);
    Map<Long, Long> jobToWorkerId = jobToWorkerId(rebalancingJobGroup.getJobs().values());
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertNotEquals(jobToWorkerId, jobToWorkerId(rebalancingJobGroup.getJobs().values()));
    Assert.assertEquals(
        1, calcDiff(jobToWorkerId, jobToWorkerId(rebalancingJobGroup.getJobs().values())));
    for (int i = 0; i < 5; ++i) {
      rebalancingJobGroup =
          buildRebalancingJobGroup(
              "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, i + 1, "a", 12));

      jobToWorkerId = jobToWorkerId(rebalancingJobGroup.getJobs().values());
      rpcUriJobAssigner.computeWorkerId(
          ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
      Assert.assertEquals(jobToWorkerId, jobToWorkerId(rebalancingJobGroup.getJobs().values()));

      jobToWorkerId = jobToWorkerId(rebalancingJobGroup.getJobs().values());
      rpcUriJobAssigner.computeWorkerId(
          ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
      Assert.assertEquals(jobToWorkerId, jobToWorkerId(rebalancingJobGroup.getJobs().values()));
    }
  }

  @Test
  public void testComputeWorkerIdOverloadResolveWithSpareWorker() {
    RebalancingJobGroup rebalancingJobGroup1 =
        buildRebalancingJobGroup(
            "jobGroup1",
            JobState.JOB_STATE_RUNNING,
            buildJob(1, 0, "a", 6),
            buildJob(2, 0, "a", 6),
            buildJob(3, 0, "a", 6));
    RebalancingJobGroup rebalancingJobGroup2 =
        buildRebalancingJobGroup(
            "jobGroup2",
            JobState.JOB_STATE_RUNNING,
            buildJob(4, 0, "b", 6),
            buildJob(5, 0, "b", 6),
            buildJob(6, 0, "b", 6));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2, 3, 4);
    rpcUriJobAssigner.computeWorkerId(
        ImmutableMap.of("jobGroup1", rebalancingJobGroup1, "jobGroup2", rebalancingJobGroup2),
        workerMap);
    Assert.assertTrue(rebalancingJobGroup1.isChanged());
    Assert.assertTrue(rebalancingJobGroup2.isChanged());
    Assert.assertEquals(12, getMaxLoadOfWorker(rebalancingJobGroup1.getJobs()).intValue());
    Assert.assertEquals(12, getMaxLoadOfWorker(rebalancingJobGroup2.getJobs()).intValue());

    putWorkerToMap(workerMap, 5);
    Map<Long, Long> jobToWorkerId1 = jobToWorkerId(rebalancingJobGroup1.getJobs().values());
    Map<Long, Long> jobToWorkerId2 = jobToWorkerId(rebalancingJobGroup2.getJobs().values());
    rpcUriJobAssigner.computeWorkerId(
        ImmutableMap.of("jobGroup1", rebalancingJobGroup1, "jobGroup2", rebalancingJobGroup2),
        workerMap);
    Assert.assertEquals(
        1, calcDiff(jobToWorkerId1, jobToWorkerId(rebalancingJobGroup1.getJobs().values())));
    Assert.assertEquals(
        0, calcDiff(jobToWorkerId2, jobToWorkerId(rebalancingJobGroup2.getJobs().values())));
    Assert.assertEquals(6, getMaxLoadOfWorker(rebalancingJobGroup1.getJobs()).intValue());
    Assert.assertEquals(12, getMaxLoadOfWorker(rebalancingJobGroup2.getJobs()).intValue());

    putWorkerToMap(workerMap, 6);
    jobToWorkerId1 = jobToWorkerId(rebalancingJobGroup1.getJobs().values());
    jobToWorkerId2 = jobToWorkerId(rebalancingJobGroup2.getJobs().values());
    rpcUriJobAssigner.computeWorkerId(
        ImmutableMap.of("jobGroup1", rebalancingJobGroup1, "jobGroup2", rebalancingJobGroup2),
        workerMap);
    Assert.assertEquals(jobToWorkerId1, jobToWorkerId(rebalancingJobGroup1.getJobs().values()));
    Assert.assertNotEquals(jobToWorkerId2, jobToWorkerId(rebalancingJobGroup2.getJobs().values()));
    Assert.assertEquals(6, getMaxLoadOfWorker(rebalancingJobGroup1.getJobs()).intValue());
    Assert.assertEquals(6, getMaxLoadOfWorker(rebalancingJobGroup2.getJobs()).intValue());
  }

  @Test
  public void testComputeWorkerIdActiveWorkerWithoutAssignment() {
    RebalancingJobGroup rebalancingJobGroup1 =
        buildRebalancingJobGroup(
            "jobGroup1",
            JobState.JOB_STATE_RUNNING,
            buildJob(1, 0, "a", 3),
            buildJob(2, 0, "a", 3),
            buildJob(3, 0, "a", 3));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1);
    rpcUriJobAssigner.computeWorkerId(
        ImmutableMap.of("jobGroup1", rebalancingJobGroup1), workerMap);
    Assert.assertTrue(rebalancingJobGroup1.isChanged());
    Assert.assertEquals(9, getMaxLoadOfWorker(rebalancingJobGroup1.getJobs()).intValue());
    Assert.assertEquals(
        1,
        rebalancingJobGroup1
            .getJobs()
            .values()
            .stream()
            .map(StoredJob::getWorkerId)
            .collect(Collectors.toSet())
            .size());

    Mockito.verify(mockSpareWorkerGauge, Mockito.times(1)).update(0.);
    Mockito.verify(mockTargetWorkerGauge, Mockito.times(1)).update(6);

    // KAFEP-910
    // Verifies computeWorkerId report correct spare worker count. which is:
    // When the cluster have 2 workers, all been assigned to a rpc uri, but one of the worker for
    // the rpc uri don't have assignment, the spare worker reports 0
    // rebalancer will rebalance the workload to the idle worker
    putWorkerToMap(workerMap, 2);
    Map<Long, Long> jobToWorkerId1 = jobToWorkerId(rebalancingJobGroup1.getJobs().values());
    rpcUriJobAssigner.computeWorkerId(
        ImmutableMap.of("jobGroup1", rebalancingJobGroup1), workerMap);
    Assert.assertNotEquals(
        0, calcDiff(jobToWorkerId1, jobToWorkerId(rebalancingJobGroup1.getJobs().values())));
    Assert.assertEquals(6, getMaxLoadOfWorker(rebalancingJobGroup1.getJobs()).intValue());

    Assert.assertEquals(
        2,
        rebalancingJobGroup1
            .getJobs()
            .values()
            .stream()
            .map(StoredJob::getWorkerId)
            .collect(Collectors.toSet())
            .size());

    Mockito.verify(mockSpareWorkerGauge, Mockito.times(2)).update(0);
    Mockito.verify(mockTargetWorkerGauge, Mockito.times(1)).update(10);

    // When the cluster have 3 workers, two of them been assigned to a rpc uri, but one of the
    // worker
    // for the rpc uri don't have assignment, the spare worker reports 1
    // no rebalance should happen
    putWorkerToMap(workerMap, 3);
    jobToWorkerId1 = jobToWorkerId(rebalancingJobGroup1.getJobs().values());
    rpcUriJobAssigner.computeWorkerId(
        ImmutableMap.of("jobGroup1", rebalancingJobGroup1), workerMap);
    Assert.assertEquals(
        0, calcDiff(jobToWorkerId1, jobToWorkerId(rebalancingJobGroup1.getJobs().values())));
    Assert.assertEquals(6, getMaxLoadOfWorker(rebalancingJobGroup1.getJobs()).intValue());

    Assert.assertEquals(
        2,
        rebalancingJobGroup1
            .getJobs()
            .values()
            .stream()
            .map(StoredJob::getWorkerId)
            .collect(Collectors.toSet())
            .size());
    Mockito.verify(mockSpareWorkerGauge, Mockito.times(1)).update(1);
    Mockito.verify(mockTargetWorkerGauge, Mockito.times(2)).update(10);
  }

  @Test
  public void testComputeWorkerIdAssignsToSpareWorkerNewJobfirst() {
    RebalancingJobGroup rebalancingJobGroup1 =
        buildRebalancingJobGroup(
            "jobGroup1",
            JobState.JOB_STATE_RUNNING,
            buildJob(1, 1),
            buildJob(2, 1),
            buildJob(3, 1));
    RebalancingJobGroup rebalancingJobGroup2 =
        buildRebalancingJobGroup(
            "jobGroup2", JobState.JOB_STATE_RUNNING, buildJob(4, 0), buildJob(5, 0));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2, 3);
    rpcUriJobAssigner.computeWorkerId(
        ImmutableMap.of("jobGroup1", rebalancingJobGroup1, "jobGroup2", rebalancingJobGroup2),
        workerMap);
    Assert.assertTrue(rebalancingJobGroup1.isChanged());
    Assert.assertTrue(rebalancingJobGroup2.isChanged());
    Assert.assertEquals(ImmutableSet.of(1L), getWorkersIds(rebalancingJobGroup1.getJobs()));
    Assert.assertEquals(ImmutableSet.of(2L, 3L), getWorkersIds(rebalancingJobGroup2.getJobs()));
  }

  @Test
  public void testComputeWorkerIdAssignsToSpareWorker() {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(
            "jobGroup", JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 1), buildJob(3, 1));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2, 3);
    rpcUriJobAssigner.computeWorkerId(ImmutableMap.of("jobGroup", rebalancingJobGroup), workerMap);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    Assert.assertEquals(ImmutableSet.of(1L, 2L, 3L), getWorkersIds(rebalancingJobGroup.getJobs()));
  }

  @Test
  public void testReportAndLogNewJobStates() {
    ImmutableMap.Builder<Long, JobState> builder = ImmutableMap.builder();
    builder.put(1L, JobState.UNRECOGNIZED);
    builder.put(2L, JobState.JOB_STATE_INVALID);
    builder.put(3L, JobState.JOB_STATE_UNIMPLEMENTED);
    builder.put(4L, JobState.JOB_STATE_FAILED);
    builder.put(5L, JobState.JOB_STATE_RUNNING);
    builder.put(6L, JobState.JOB_STATE_CANCELED);
    rpcUriJobAssigner.reportAndLogNewJobStates(builder.build());
  }

  @Test
  public void testRebalancingJob() {
    RebalancingJobGroup jobGroup = Mockito.mock(RebalancingJobGroup.class);
    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    jobBuilder.setState(JobState.JOB_STATE_RUNNING);
    jobBuilder.getJobBuilder().getRpcDispatcherTaskBuilder().setUri("grpc://uri");
    jobBuilder.getJobBuilder().setJobId(2);
    jobBuilder.setScale(0.2);
    jobBuilder.getJobBuilder().getFlowControlBuilder().setMessagesPerSec(2);
    RebalancingJob job = new RebalancingJob(jobBuilder.build(), jobGroup);
    jobBuilder.getJobBuilder().setJobId(1);
    jobBuilder.setScale(0.1);
    jobBuilder.getJobBuilder().getFlowControlBuilder().setMessagesPerSec(1);
    RebalancingJob smallerJob = new RebalancingJob(jobBuilder.build(), jobGroup);
    jobBuilder.getJobBuilder().setJobId(3);
    jobBuilder.setScale(0.3);
    jobBuilder.getJobBuilder().getFlowControlBuilder().setMessagesPerSec(3);
    RebalancingJob largerJob = new RebalancingJob(jobBuilder.build(), jobGroup);

    Assert.assertEquals(0, job.getWorkerId());
    Assert.assertEquals("grpc://uri", job.getRpcUri());
    Assert.assertEquals(JobState.JOB_STATE_RUNNING, job.getJobState());
    Assert.assertEquals(2.0 / messagesPerSecPerWorker, job.getLoad(), 0.0);
    Assert.assertEquals(job, job);
    Assert.assertEquals(job.hashCode(), job.hashCode());
    Assert.assertNotEquals(job, smallerJob);
    Assert.assertNotEquals(job.hashCode(), smallerJob.hashCode());
    Assert.assertNotEquals(job, jobGroup);
    Assert.assertTrue(job.compareTo(smallerJob) < 0);
    Assert.assertTrue(job.compareTo(largerJob) > 0);

    jobBuilder.getJobBuilder().setJobId(2);
    jobBuilder.setScale(0.2);
    jobBuilder.getJobBuilder().getFlowControlBuilder().setMessagesPerSec(2);
    jobBuilder.setWorkerId(4);
    job.setWorkerId(4);
    Mockito.verify(jobGroup, Mockito.times(1)).updateJob(2L, jobBuilder.build());
  }

  @Test
  public void testRebalancingWorker() {
    RebalancingWorker worker = new RebalancingWorker(2, 2);
    RebalancingWorker smallerWorker = new RebalancingWorker(1, 1);
    RebalancingWorker largerWorker = new RebalancingWorker(3, 3);

    Assert.assertEquals(2, worker.getWorkerId());
    Assert.assertEquals(2.0, worker.getLoad(), 0.0);
    Assert.assertEquals(worker, worker);
    Assert.assertEquals(worker.hashCode(), worker.hashCode());
    Assert.assertNotEquals(worker, smallerWorker);
    Assert.assertNotEquals(worker.hashCode(), smallerWorker.hashCode());
    Assert.assertNotEquals(worker, StoredJob.getDefaultInstance());
    Assert.assertTrue(worker.compareTo(smallerWorker) > 0);
    Assert.assertTrue(worker.compareTo(largerWorker) < 0);
  }

  @Test
  public void testRoundUpToNearestNumber() {
    RebalancerConfiguration config = new RebalancerConfiguration();
    config.setNumWorkersPerUri(2);
    config.setMessagesPerSecPerWorker(4000);
    AbstractRpcUriRebalancer rebalancer = new SimpleRpcUriAssigner(new NoopScope(), config, scalar);
    Assert.assertEquals(0, roundUpToNearestNumber(0, 5));
    Assert.assertEquals(10, roundUpToNearestNumber(9, 5));
    Assert.assertEquals(10, roundUpToNearestNumber(10, 5));
    Assert.assertEquals(15, roundUpToNearestNumber(11, 5));
  }

  private class SimpleRpcUriAssigner extends AbstractRpcUriRebalancer {
    SimpleRpcUriAssigner(Scope scope, RebalancerConfiguration config, Scalar scalar) {
      super(scope, config, scalar, hibernatingJobRebalancer);
    }
  }
}
