package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.ScaleStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Throughput;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RebalancingJobGroupTest {
  private StoredJob jobOne;
  private StoredJob jobTwo;
  private StoredJobGroup jobGroup;
  private StoredJobStatus jobStatus;
  private RebalancingJobGroup rebalancingJobGroup;

  @BeforeEach
  public void setup() {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    jobBuilder
        .getJobBuilder()
        .setFlowControl(FlowControl.newBuilder().setMessagesPerSec(1.0).build());
    jobBuilder.getJobBuilder().setJobId(1L);
    jobOne = jobBuilder.build();
    jobBuilder.getJobBuilder().setJobId(2L);
    jobTwo = jobBuilder.build();
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.addJobs(jobOne);
    jobGroupBuilder.addJobs(jobTwo);
    jobGroupBuilder.setState(JobState.JOB_STATE_RUNNING);
    jobGroupBuilder
        .getJobGroupBuilder()
        .setFlowControl(FlowControl.newBuilder().setMessagesPerSec(2.0).build());
    jobGroupBuilder.setScaleStatus(ScaleStatus.newBuilder().setScale(1.5).build());
    jobGroup = jobGroupBuilder.build();
    StoredJobStatus.Builder jobStatusBuilder = StoredJobStatus.newBuilder();
    jobStatusBuilder.getJobStatusBuilder().setJob(jobOne.getJob());
    jobStatus = jobStatusBuilder.build();
    rebalancingJobGroup =
        RebalancingJobGroup.of(Versioned.from(jobGroup, 1), ImmutableMap.of(1L, jobStatus));
    Assertions.assertFalse(rebalancingJobGroup.isChanged());
  }

  @Test
  public void testGetJobGroup() {
    Assertions.assertEquals(jobGroup.getJobGroup(), rebalancingJobGroup.getJobGroup());
  }

  @Test
  public void testGetJobGroupState() {
    Assertions.assertEquals(JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testGetJobs() {
    Assertions.assertEquals(ImmutableMap.of(1L, jobOne, 2L, jobTwo), rebalancingJobGroup.getJobs());
  }

  @Test
  public void testGetJobStatusMap() {
    Assertions.assertEquals(ImmutableMap.of(1L, jobStatus), rebalancingJobGroup.getJobStatusMap());
  }

  @Test
  public void testUpdateJob() {
    StoredJob newJobOne =
        jobOne.toBuilder().setWorkerId(3).setState(JobState.JOB_STATE_CANCELED).build();
    StoredJobGroup newJobGroup =
        jobGroup.toBuilder().clearJobs().addJobs(newJobOne).addJobs(jobTwo).build();
    Assertions.assertTrue(rebalancingJobGroup.updateJob(1L, newJobOne));
    Assertions.assertTrue(rebalancingJobGroup.isChanged());
    Versioned<StoredJobGroup> got = rebalancingJobGroup.toStoredJobGroup();
    Assertions.assertEquals(1, got.version());
    Assertions.assertEquals(newJobGroup, got.model());
  }

  @Test
  public void testUpdateJobSkipForWrongKey() {
    StoredJob newJobOne = jobOne.toBuilder().setState(JobState.JOB_STATE_CANCELED).build();
    Assertions.assertFalse(rebalancingJobGroup.updateJob(3L, newJobOne));
    Assertions.assertFalse(rebalancingJobGroup.isChanged());
    Versioned<StoredJobGroup> got = rebalancingJobGroup.toStoredJobGroup();
    Assertions.assertEquals(1, got.version());
    Assertions.assertEquals(jobGroup, got.model());
  }

  @Test
  public void testUpdateJobGroupState() {
    Assertions.assertFalse(rebalancingJobGroup.updateJobGroupState(JobState.JOB_STATE_RUNNING));
    Assertions.assertEquals(JobState.JOB_STATE_RUNNING, rebalancingJobGroup.getJobGroupState());
    Assertions.assertTrue(rebalancingJobGroup.updateJobGroupState(JobState.JOB_STATE_CANCELED));
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, rebalancingJobGroup.getJobGroupState());
  }

  @Test
  public void testFilterByJobGroupState() {
    Assertions.assertTrue(
        RebalancingJobGroup.filterByJobGroupState(ImmutableSet.of(JobState.JOB_STATE_RUNNING))
            .test(rebalancingJobGroup));
    Assertions.assertFalse(
        RebalancingJobGroup.filterByJobGroupState(ImmutableSet.of(JobState.JOB_STATE_CANCELED))
            .test(rebalancingJobGroup));
  }

  @Test
  public void testUpdateScale() {
    boolean updated = rebalancingJobGroup.updateScale(1.5, new Throughput(1.5d, 1.5d));
    Assertions.assertFalse(updated);
    Assertions.assertFalse(rebalancingJobGroup.isChanged());
    updated = rebalancingJobGroup.updateScale(3.0, new Throughput(3.0d, 3.0d));
    Assertions.assertTrue(updated);
    Assertions.assertTrue(rebalancingJobGroup.isChanged());
    Assertions.assertEquals(
        3.0, rebalancingJobGroup.toStoredJobGroup().model().getScaleStatus().getScale(), 0.001);
  }
}
