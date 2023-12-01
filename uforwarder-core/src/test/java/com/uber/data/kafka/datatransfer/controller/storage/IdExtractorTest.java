package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IdExtractorTest extends FievelTestBase {
  private IdProvider<Long, StoredJobStatus> idExtractor;

  @Before
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
    Assert.assertEquals(2L, idExtractor.getId(storedJobStatus).longValue());
  }

  @Test
  public void testNoError() {
    idExtractor.start();
    idExtractor.stop();
    idExtractor.isRunning();
  }
}
