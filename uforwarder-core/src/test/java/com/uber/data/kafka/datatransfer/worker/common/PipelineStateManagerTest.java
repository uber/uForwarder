package com.uber.data.kafka.datatransfer.worker.common;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineHealthIssue;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineLoadTracker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PipelineStateManagerTest {
  private PipelineStateManager pipelineStateManager;

  @BeforeEach
  public void setUp() {
    pipelineStateManager = new PipelineStateManager() {};
  }

  @Test
  public void testUpdateActualRunningJobStatus() {
    ImmutableList<JobStatus> jobStatusList = ImmutableList.of(JobStatus.newBuilder().build());
    pipelineStateManager.updateActualRunningJobStatus(jobStatusList);
  }

  @Test
  public void testGetExpectedRunningJobMap() {
    Assertions.assertTrue(pipelineStateManager.getExpectedRunningJobMap().isEmpty());
  }

  @Test
  public void testShouldJobBeRunning() {
    Assertions.assertFalse(pipelineStateManager.shouldJobBeRunning(Job.getDefaultInstance()));
  }

  @Test
  public void testGetExpectedJob() {
    Assertions.assertFalse(pipelineStateManager.getExpectedJob(1).isPresent());
  }

  @Test
  public void testGetQuota() {
    Assertions.assertEquals(
        FlowControl.newBuilder().build(), pipelineStateManager.getFlowControl());
  }

  @Test
  public void testGetJobDefinitionTemplate() {
    Assertions.assertEquals(Job.newBuilder().build(), pipelineStateManager.getJobTemplate());
  }

  @Test
  public void testClear() {
    pipelineStateManager.clear();
  }

  @Test
  public void testGetJobs() {
    Assertions.assertEquals(0, pipelineStateManager.getJobs().size());
  }

  @Test
  public void testReportIssue() {
    pipelineStateManager.reportIssue(
        Mockito.mock(Job.class), Mockito.mock(PipelineHealthIssue.class));
  }

  @Test
  public void testGetLoadTracker() {
    Assertions.assertEquals(PipelineLoadTracker.NOOP, pipelineStateManager.getLoadTracker());
  }
}
