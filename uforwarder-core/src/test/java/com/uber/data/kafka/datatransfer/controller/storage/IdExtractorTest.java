package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class IdExtractorTest {
  private IdProvider<Long, StoredJobStatus> idExtractor;

  @BeforeEach
  public void setUp() {
    idExtractor = new IdExtractor<>(j -> j.getJobStatus().getJob().getJobId());
  }

  @Test
  public void testExtractId() throws Exception {
    StoredJobStatus storedJobStatus =
        StoredJobStatus.newBuilder()
            .setJobStatus(
                JobStatus.newBuilder().setJob(Job.newBuilder().setJobId(2).build()).build())
            .build();
    Assertions.assertEquals(2L, idExtractor.getId(storedJobStatus).longValue());
  }

  @Test
  public void testNoError() {
    idExtractor.start();
    idExtractor.stop();
    idExtractor.isRunning();
  }
}
