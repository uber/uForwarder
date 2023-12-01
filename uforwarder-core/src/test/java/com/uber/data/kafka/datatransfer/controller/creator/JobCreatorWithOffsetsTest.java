package com.uber.data.kafka.datatransfer.controller.creator;

import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

public class JobCreatorWithOffsetsTest extends FievelTestBase {
  private JobCreatorWithOffsets jobCreatorWithOffsets;
  private Scope scope;
  private Logger logger;
  private StoredJobGroup storedJobGroup;
  private String jobType;

  @Before
  public void setUp() {
    jobCreatorWithOffsets = new JobCreatorWithOffsets() {};
    scope = new NoopScope();
    logger = NOPLogger.NOP_LOGGER;
    storedJobGroup = StoredJobGroup.newBuilder().setState(JobState.JOB_STATE_RUNNING).build();
    jobType = "STREAMING";
  }

  @Test
  public void testNewJobSmallerStartOffset() {
    StoredJob newJob =
        jobCreatorWithOffsets.newJob(scope, logger, storedJobGroup, jobType, 10, 20, 30, 40);
    Assert.assertEquals(10, newJob.getJob().getJobId());
    Assert.assertEquals(20, newJob.getJob().getKafkaConsumerTask().getPartition());
    Assert.assertEquals(30, newJob.getJob().getKafkaConsumerTask().getStartOffset());
    Assert.assertEquals(40, newJob.getJob().getKafkaConsumerTask().getEndOffset());
    Assert.assertEquals(JobState.JOB_STATE_RUNNING, newJob.getState());
  }

  @Test
  public void testNewJobSameOffset() {
    StoredJob newJob =
        jobCreatorWithOffsets.newJob(scope, logger, storedJobGroup, jobType, 10, 20, 40, 40);
    Assert.assertEquals(10, newJob.getJob().getJobId());
    Assert.assertEquals(20, newJob.getJob().getKafkaConsumerTask().getPartition());
    Assert.assertEquals(40, newJob.getJob().getKafkaConsumerTask().getStartOffset());
    Assert.assertEquals(40, newJob.getJob().getKafkaConsumerTask().getEndOffset());
    Assert.assertEquals(JobState.JOB_STATE_CANCELED, newJob.getState());
  }

  @Test
  public void testNewJobLargerStartOffset() {
    StoredJob newJob =
        jobCreatorWithOffsets.newJob(scope, logger, storedJobGroup, jobType, 10, 20, 50, 40);
    Assert.assertEquals(10, newJob.getJob().getJobId());
    Assert.assertEquals(20, newJob.getJob().getKafkaConsumerTask().getPartition());
    Assert.assertEquals(50, newJob.getJob().getKafkaConsumerTask().getStartOffset());
    Assert.assertEquals(40, newJob.getJob().getKafkaConsumerTask().getEndOffset());
    Assert.assertEquals(JobState.JOB_STATE_CANCELED, newJob.getState());
  }
}
