package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.data.kafka.datatransfer.controller.rebalancer.ShadowRebalancerDelegate;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.HashMap;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ShadowRebalancerDelegateImplTest extends FievelTestBase {
  private ShadowRebalancerDelegate shadowRebalancerDelegate;
  private Rebalancer shadowRebalancer;

  @Before
  public void setup() throws Exception {
    shadowRebalancer = Mockito.mock(Rebalancer.class);
    shadowRebalancerDelegate = new ShadowRebalancerDelegateImpl(shadowRebalancer, true);
  }

  @Test
  public void testComputeLoad() throws Exception {
    doNothing().when(shadowRebalancer).computeLoad(any());
    shadowRebalancerDelegate.computeLoad(ImmutableMap.of());
    verify(shadowRebalancer, times(1)).computeLoad(any());
  }

  @Test
  public void testComputeJobConfiguration() throws Exception {
    doNothing().when(shadowRebalancer).computeJobConfiguration(any(), any());
    shadowRebalancerDelegate.computeJobConfiguration(ImmutableMap.of(), ImmutableMap.of());
    verify(shadowRebalancer, times(1)).computeJobConfiguration(any(), any());
  }

  @Test
  public void testComputeJobState() throws Exception {
    doNothing().when(shadowRebalancer).computeJobState(any(), any());
    shadowRebalancerDelegate.computeJobState(ImmutableMap.of(), ImmutableMap.of());
    verify(shadowRebalancer, times(1)).computeJobState(any(), any());
  }

  @Test
  public void testRunShaodowRebalancer() throws Exception {
    Assert.assertTrue(shadowRebalancerDelegate.runShadowRebalancer());
  }

  @Test
  public void testComputeWorkerIdWithFreshCache() throws Exception {
    Rebalancer customizedRebalancer =
        new Rebalancer() {
          @Override
          public void computeWorkerId(
              Map<String, RebalancingJobGroup> jobGroups, Map<Long, StoredWorker> workers)
              throws Exception {
            for (Map.Entry<String, RebalancingJobGroup> jobGroupEntry : jobGroups.entrySet()) {
              for (StoredJob job : jobGroupEntry.getValue().getJobs().values()) {
                long existingWorkerId = job.getWorkerId();
                jobGroupEntry
                    .getValue()
                    .updateJob(
                        job.getJob().getJobId(),
                        job.toBuilder().setWorkerId(2 * existingWorkerId).build());
              }
            }
          }
        };

    // we will have 6 workers
    Map<Long, StoredWorker> workerMap = new HashMap<>();
    for (int i = 0; i < 6; i++) {
      StoredWorker.Builder workerBuilder = StoredWorker.newBuilder();
      workerBuilder.getNodeBuilder().setId(3 + i);
      StoredWorker worker = workerBuilder.build();
      workerMap.put(3L + i, worker);
    }

    Map<String, RebalancingJobGroup> jobGroupMap1 = buildJobGroup("jobGroup1", 3);
    Map<String, RebalancingJobGroup> jobGroupMap2 = buildJobGroup("jobGroup2", 5);
    Map<String, RebalancingJobGroup> inputJobGroup = new HashMap<>();
    inputJobGroup.putAll(jobGroupMap1);
    inputJobGroup.putAll(jobGroupMap2);
    ShadowRebalancerDelegateImpl customizedShadowRebalacerDelegate =
        new ShadowRebalancerDelegateImpl(customizedRebalancer, true);
    customizedShadowRebalacerDelegate.computeWorkerId(inputJobGroup, workerMap);

    Map<String, Map<Long, Long>> cachedJobGroupStatus =
        customizedShadowRebalacerDelegate.getCachedJobGroupStatus();
    Map<Long, Long> jobWorkerMap = cachedJobGroupStatus.get("jobGroup1");
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(jobWorkerMap.get(1L + i).longValue(), 2 * (3L + i * 2L));
    }
    jobWorkerMap = cachedJobGroupStatus.get("jobGroup2");
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(jobWorkerMap.get(1L + i).longValue(), 2 * (3L + i * 2L));
    }
  }

  @Test
  public void testComputeWorkerIdWithNewJobs() throws Exception {
    Rebalancer customizedRebalancer =
        new Rebalancer() {
          @Override
          public void computeWorkerId(
              Map<String, RebalancingJobGroup> jobGroups, Map<Long, StoredWorker> workers)
              throws Exception {
            for (Map.Entry<String, RebalancingJobGroup> jobGroupEntry : jobGroups.entrySet()) {
              for (StoredJob job : jobGroupEntry.getValue().getJobs().values()) {
                long existingWorkerId = job.getWorkerId();
                jobGroupEntry
                    .getValue()
                    .updateJob(
                        job.getJob().getJobId(),
                        job.toBuilder().setWorkerId(2 * existingWorkerId).build());
              }
            }
          }
        };

    // we will have 15 workers
    Map<Long, StoredWorker> workerMap = new HashMap<>();
    for (int i = 0; i < 15; i++) {
      StoredWorker.Builder workerBuilder = StoredWorker.newBuilder();
      workerBuilder.getNodeBuilder().setId(3 + i);
      StoredWorker worker = workerBuilder.build();
      workerMap.put(3L + i, worker);
    }

    Map<String, RebalancingJobGroup> jobGroupMap1 = buildJobGroup("jobGroup1", 3);
    ShadowRebalancerDelegateImpl customizedShadowRebalacerDelegate =
        new ShadowRebalancerDelegateImpl(customizedRebalancer, true);
    customizedShadowRebalacerDelegate.computeWorkerId(jobGroupMap1, workerMap);

    // change worker id
    StoredJob job1 = jobGroupMap1.get("jobGroup1").getJobs().get(1L);
    jobGroupMap1.get("jobGroup1").updateJob(1L, job1.toBuilder().setWorkerId(1L).build());
    StoredJob job2 = jobGroupMap1.get("jobGroup1").getJobs().get(2L);
    jobGroupMap1.get("jobGroup1").updateJob(2L, job2.toBuilder().setWorkerId(2L).build());
    StoredJob job3 = jobGroupMap1.get("jobGroup1").getJobs().get(3L);
    jobGroupMap1.get("jobGroup1").updateJob(3L, job3.toBuilder().setWorkerId(6L).build());

    Map<String, Map<Long, Long>> cachedJobGroupStatus =
        customizedShadowRebalacerDelegate.getCachedJobGroupStatus();
    // also remove job 1
    cachedJobGroupStatus.get("jobGroup1").remove(1L);

    customizedShadowRebalacerDelegate.computeWorkerId(jobGroupMap1, workerMap);
    Map<Long, Long> jobWorkerMap = cachedJobGroupStatus.get("jobGroup1");
    // job1 should use 1L as worker id in rebalance, job2 and 3 should use the cached worker id in
    // rebalance
    Assert.assertEquals(jobWorkerMap.get(1L).longValue(), 2L);
    Assert.assertEquals(jobWorkerMap.get(2L).longValue(), 20L);
    Assert.assertEquals(jobWorkerMap.get(3L).longValue(), 28L);
  }

  private Map<String, RebalancingJobGroup> buildJobGroup(String jobGroupId, int numberOfJobs) {
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder();
    Map<Long, StoredJobStatus> jobStatusMap = new HashMap<>();
    StoredJobStatus jobStatus =
        StoredJobStatus.newBuilder()
            .setJobStatus(JobStatus.newBuilder().setState(JobState.JOB_STATE_RUNNING).build())
            .build();
    for (int i = 0; i < numberOfJobs; i++) {
      StoredJob.Builder jobBuilder = StoredJob.newBuilder();
      jobBuilder.getJobBuilder().setJobId(1L + i);
      jobBuilder.setWorkerId(3L + i * 2L);
      StoredJob job = jobBuilder.build();
      builder.addJobs(job);
      jobStatusMap.put(1L + i, jobStatus);
    }

    StoredJobGroup storedJobGroup = builder.build();

    RebalancingJobGroup rebalancingJobGroup =
        RebalancingJobGroup.of(Versioned.from(storedJobGroup, 1), jobStatusMap);
    return ImmutableMap.of(jobGroupId, rebalancingJobGroup);
  }
}
