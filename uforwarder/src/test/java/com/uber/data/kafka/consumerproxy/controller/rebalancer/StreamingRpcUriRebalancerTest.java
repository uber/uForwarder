package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.controller.autoscalar.AutoScalar;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import java.util.Collections;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class StreamingRpcUriRebalancerTest extends FievelTestBase {
  Rebalancer streamingRpcUriRebalancer;
  AutoScalar autoScalar;
  private HibernatingJobRebalancer hibernatingJobRebalancer;

  @Before
  public void setup() {
    RebalancerConfiguration config = new RebalancerConfiguration();
    autoScalar = Mockito.mock(AutoScalar.class);
    hibernatingJobRebalancer = Mockito.mock(HibernatingJobRebalancer.class);
    Mockito.doAnswer(
            invocation -> {
              return Collections.EMPTY_SET;
            })
        .when(hibernatingJobRebalancer)
        .computeWorkerId(Mockito.anyList(), Mockito.anyMap());
    streamingRpcUriRebalancer =
        new StreamingRpcUriRebalancer(
            new NoopScope(), config, autoScalar, hibernatingJobRebalancer);
  }

  @Test
  public void testComputeJobStateUnimplementedToRunning() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(1L, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_UNIMPLEMENTED);
    streamingRpcUriRebalancer.computeJobState(
        ImmutableMap.of("jobGroup", rebalancingJobGroup), ImmutableMap.of());
    Assert.assertEquals(
        JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobs().get(1L).getState());
  }

  @Test
  public void testComputeJobStateInvalidToRunning() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(1L, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_INVALID);
    streamingRpcUriRebalancer.computeJobState(
        ImmutableMap.of("jobGroup", rebalancingJobGroup), ImmutableMap.of());
    Assert.assertEquals(
        JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobs().get(1L).getState());
  }

  @Test
  public void testComputeJobStateFailedToRunning() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(1L, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_FAILED);
    streamingRpcUriRebalancer.computeJobState(
        ImmutableMap.of("jobGroup", rebalancingJobGroup), ImmutableMap.of());
    Assert.assertEquals(
        JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobs().get(1L).getState());
  }

  @Test
  public void testComputeJobStateRunningToRunning() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(1L, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_RUNNING);
    streamingRpcUriRebalancer.computeJobState(
        ImmutableMap.of("jobGroup", rebalancingJobGroup), ImmutableMap.of());
    Assert.assertEquals(
        JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobs().get(1L).getState());
  }

  @Test
  public void testComputeJobStateCanceledToCanceled() throws Exception {
    RebalancingJobGroup rebalancingJobGroup =
        buildRebalancingJobGroup(1L, JobState.JOB_STATE_RUNNING, JobState.JOB_STATE_CANCELED);
    streamingRpcUriRebalancer.computeJobState(
        ImmutableMap.of("jobGroup", rebalancingJobGroup), ImmutableMap.of());
    Assert.assertEquals(
        JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobs().get(1L).getState());
  }

  private RebalancingJobGroup buildRebalancingJobGroup(
      long jobId, JobState jobGroupState, JobState expected) {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    jobBuilder.getJobBuilder().setJobId(jobId);
    jobBuilder.setState(expected);
    StoredJob job = jobBuilder.build();
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.setState(jobGroupState);
    jobGroupBuilder.addJobs(job);
    StoredJobGroup jobGroup = jobGroupBuilder.build();
    return RebalancingJobGroup.of(Versioned.from(jobGroup, 0), ImmutableMap.of());
  }
}
