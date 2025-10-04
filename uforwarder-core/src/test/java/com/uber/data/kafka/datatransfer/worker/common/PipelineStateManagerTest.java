package com.uber.data.kafka.datatransfer.worker.common;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineHealthIssue;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PipelineStateManagerTest extends FievelTestBase {
  private PipelineStateManager pipelineStateManager;

  @Before
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
    Assert.assertTrue(pipelineStateManager.getExpectedRunningJobMap().isEmpty());
  }

  @Test
  public void testShouldJobBeRunning() {
    Assert.assertFalse(pipelineStateManager.shouldJobBeRunning(Job.getDefaultInstance()));
  }

  @Test
  public void testGetExpectedJob() {
    Assert.assertFalse(pipelineStateManager.getExpectedJob(1).isPresent());
  }

  @Test
  public void testGetQuota() {
    Assert.assertEquals(FlowControl.newBuilder().build(), pipelineStateManager.getFlowControl());
  }

  @Test
  public void testGetJobDefinitionTemplate() {
    Assert.assertEquals(Job.newBuilder().build(), pipelineStateManager.getJobTemplate());
  }

  @Test
  public void testClear() {
    pipelineStateManager.clear();
  }

  @Test
  public void testGetJobs() {
    Assert.assertEquals(0, pipelineStateManager.getJobs().size());
  }

  @Test
  public void testReportIssue() {
    pipelineStateManager.reportIssue(
        Mockito.mock(Job.class), Mockito.mock(PipelineHealthIssue.class));
  }
}
