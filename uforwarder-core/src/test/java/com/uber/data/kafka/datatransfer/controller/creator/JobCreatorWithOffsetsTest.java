package com.uber.data.kafka.datatransfer.controller.creator;

import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

public class JobCreatorWithOffsetsTest {
  private JobCreatorWithOffsets jobCreatorWithOffsets;
  private Scope scope;
  private Logger logger;
  private StoredJobGroup storedJobGroup;
  private String jobType;

  @BeforeEach
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
        jobCreatorWithOffsets.newJob(
            scope,
            logger,
            storedJobGroup,
            jobType,
            10,
            20,
            new JobCreatorWithOffsets.OffsetRange(30, 40));
    Assertions.assertEquals(10, newJob.getJob().getJobId());
    Assertions.assertEquals(20, newJob.getJob().getKafkaConsumerTask().getPartition());
    Assertions.assertEquals(30, newJob.getJob().getKafkaConsumerTask().getStartOffset());
    Assertions.assertEquals(40, newJob.getJob().getKafkaConsumerTask().getEndOffset());
    Assertions.assertEquals(JobState.JOB_STATE_RUNNING, newJob.getState());
  }

  @Test
  public void testNewJobSameOffset() {
    StoredJob newJob =
        jobCreatorWithOffsets.newJob(
            scope,
            logger,
            storedJobGroup,
            jobType,
            10,
            20,
            new JobCreatorWithOffsets.OffsetRange(40, 40));
    Assertions.assertEquals(10, newJob.getJob().getJobId());
    Assertions.assertEquals(20, newJob.getJob().getKafkaConsumerTask().getPartition());
    Assertions.assertEquals(40, newJob.getJob().getKafkaConsumerTask().getStartOffset());
    Assertions.assertEquals(40, newJob.getJob().getKafkaConsumerTask().getEndOffset());
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, newJob.getState());
  }

  @Test
  public void testNewJobLargerStartOffset() {
    StoredJob newJob =
        jobCreatorWithOffsets.newJob(
            scope,
            logger,
            storedJobGroup,
            jobType,
            10,
            20,
            new JobCreatorWithOffsets.OffsetRange(50, 40));
    Assertions.assertEquals(10, newJob.getJob().getJobId());
    Assertions.assertEquals(20, newJob.getJob().getKafkaConsumerTask().getPartition());
    Assertions.assertEquals(50, newJob.getJob().getKafkaConsumerTask().getStartOffset());
    Assertions.assertEquals(40, newJob.getJob().getKafkaConsumerTask().getEndOffset());
    Assertions.assertEquals(JobState.JOB_STATE_CANCELED, newJob.getState());
  }
}
