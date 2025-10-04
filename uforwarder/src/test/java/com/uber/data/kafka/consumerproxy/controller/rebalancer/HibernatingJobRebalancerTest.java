package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HibernatingJobRebalancerTest extends FievelTestBase {
  private HibernatingJobRebalancer hibernatingJobRebalancer;
  private RebalancerConfiguration configuration;

  @Before
  public void setup() {
    configuration = new RebalancerConfiguration();
    configuration.setHibernatingJobGroupPerWorker(2);
    hibernatingJobRebalancer = new HibernatingJobRebalancer(configuration);
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

  private RebalancingJobGroup buildRebalancingJobGroup(JobState jobGroupState, StoredJob... jobs) {
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    for (StoredJob job : jobs) {
      jobGroupBuilder.addJobs(job.toBuilder().setState(jobGroupState).build());
    }
    StoredJobGroup jobGroup =
        jobGroupBuilder.setJobGroup(JobGroup.newBuilder().build()).setState(jobGroupState).build();
    RebalancingJobGroup result =
        RebalancingJobGroup.of(Versioned.from(jobGroup, 1), ImmutableMap.of());
    return RebalancingJobGroup.of(result.toStoredJobGroup(), ImmutableMap.of());
  }

  private static StoredJob buildJob(long jobId, long workerId) {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    jobBuilder.getJobBuilder().setJobId(jobId);
    jobBuilder.setWorkerId(workerId);
    return jobBuilder.build();
  }

  @Test
  public void testWithoutWorker() {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 1));
    Map<Long, StoredWorker> workerMap = buildWorkerMap();
    Collection<Long> usedWorkers =
        hibernatingJobRebalancer.computeWorkerId(Arrays.asList(rebalancingJobGroup), workerMap);
    Assert.assertTrue(usedWorkers.isEmpty());
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    rebalancingJobGroup.getJobs().values().stream()
        .forEach(storedJob -> Assert.assertEquals(0, storedJob.getWorkerId()));
  }

  @Test
  public void testStableAssignment() {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 1));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    Collection<Long> usedWorkers =
        hibernatingJobRebalancer.computeWorkerId(Arrays.asList(rebalancingJobGroup), workerMap);
    Assert.assertEquals(ImmutableSet.of(1L), usedWorkers);
    Assert.assertFalse(rebalancingJobGroup.isChanged());
    rebalancingJobGroup.getJobs().values().stream()
        .forEach(storedJob -> Assert.assertEquals(1, storedJob.getWorkerId()));
  }

  @Test
  public void testMoreThanEnoughSpareWorkers() {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(1, 0), buildJob(2, 0));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    Collection<Long> usedWorkers =
        hibernatingJobRebalancer.computeWorkerId(Arrays.asList(rebalancingJobGroup), workerMap);
    Assert.assertEquals(ImmutableSet.of(2L), usedWorkers);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    rebalancingJobGroup.getJobs().values().stream()
        .forEach(storedJob -> Assert.assertEquals(2, storedJob.getWorkerId()));
  }

  @Test
  public void testNoEnoughSpareWorkers() {
    RebalancingJobGroup group1 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(1, 0));
    RebalancingJobGroup group2 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(2, 0));
    RebalancingJobGroup group3 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(3, 0));
    RebalancingJobGroup group4 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(4, 0));
    RebalancingJobGroup group5 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(5, 0));
    List<RebalancingJobGroup> groups = Arrays.asList(group1, group2, group3, group4, group5);
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    Collection<Long> usedWorkers = hibernatingJobRebalancer.computeWorkerId(groups, workerMap);
    Assert.assertEquals(ImmutableSet.of(1L, 2L), usedWorkers);
    groups.stream().forEach(group -> Assert.assertTrue(group.isChanged()));
    Map<Long, Integer> jobsPerWorker = new HashMap<>();
    for (RebalancingJobGroup jobGroup : groups) {
      for (StoredJob job : jobGroup.getJobs().values()) {
        jobsPerWorker.compute(
            job.getWorkerId(), (workerId, count) -> Integer.valueOf(count == null ? 1 : count + 1));
      }
    }
    Assert.assertEquals(ImmutableMap.copyOf(jobsPerWorker), ImmutableMap.of(1L, 2, 2L, 3));
  }

  @Test
  public void testConvergeJobGroupOnWorker() {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(1, 1), buildJob(2, 2));
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    Collection<Long> usedWorkers =
        hibernatingJobRebalancer.computeWorkerId(Arrays.asList(rebalancingJobGroup), workerMap);
    Assert.assertEquals(ImmutableSet.of(1L), usedWorkers);
    Assert.assertTrue(rebalancingJobGroup.isChanged());
    rebalancingJobGroup.getJobs().values().stream()
        .forEach(storedJob -> Assert.assertEquals(1, storedJob.getWorkerId()));
  }

  @Test
  public void testOverloadedWorker() {
    RebalancingJobGroup group1 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(1, 1));
    RebalancingJobGroup group2 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(2, 1));
    RebalancingJobGroup group3 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(3, 1));
    List<RebalancingJobGroup> groups = Arrays.asList(group1, group2, group3);
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    Collection<Long> usedWorkers = hibernatingJobRebalancer.computeWorkerId(groups, workerMap);
    Assert.assertEquals(ImmutableSet.of(1L, 2L), usedWorkers);
    Assert.assertEquals(1, groups.stream().mapToInt(group -> group.isChanged() ? 1 : 0).sum());
    Map<Long, Integer> jobsPerWorker = new HashMap<>();
    for (RebalancingJobGroup jobGroup : groups) {
      for (StoredJob job : jobGroup.getJobs().values()) {
        jobsPerWorker.compute(
            job.getWorkerId(), (workerId, count) -> Integer.valueOf(count == null ? 1 : count + 1));
      }
    }
    Assert.assertEquals(ImmutableMap.copyOf(jobsPerWorker), ImmutableMap.of(1L, 2, 2L, 1));
  }

  @Test
  public void testMergeJobGroupsToWorker() {
    RebalancingJobGroup group1 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(1, 1));
    RebalancingJobGroup group2 =
        buildRebalancingJobGroup(JobState.JOB_STATE_RUNNING, buildJob(2, 2));
    List<RebalancingJobGroup> groups = Arrays.asList(group1, group2);
    Map<Long, StoredWorker> workerMap = buildWorkerMap(1, 2);
    Collection<Long> usedWorkers = hibernatingJobRebalancer.computeWorkerId(groups, workerMap);
    Assert.assertEquals(ImmutableSet.of(2L), usedWorkers);
    Assert.assertTrue(group1.isChanged());
    Assert.assertFalse(group2.isChanged());
    Map<Long, Integer> jobsPerWorker = new HashMap<>();
    for (RebalancingJobGroup jobGroup : groups) {
      for (StoredJob job : jobGroup.getJobs().values()) {
        jobsPerWorker.compute(
            job.getWorkerId(), (workerId, count) -> Integer.valueOf(count == null ? 1 : count + 1));
      }
    }
    Assert.assertEquals(ImmutableMap.copyOf(jobsPerWorker), ImmutableMap.of(2L, 2));
  }
}
