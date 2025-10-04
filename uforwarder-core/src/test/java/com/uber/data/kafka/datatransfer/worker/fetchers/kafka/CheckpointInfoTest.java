package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.Job;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CheckpointInfoTest {
  private Job job;
  private CheckpointInfo checkpointInfo;

  @BeforeEach
  public void setUp() {
    job = Job.newBuilder().build();
    checkpointInfo = new CheckpointInfo(job, 50L, 100L);
  }

  @Test
  public void testGet() {
    Assertions.assertEquals(job, checkpointInfo.getJob());
    Assertions.assertEquals(50L, checkpointInfo.getStartingOffset());
    Assertions.assertEquals(50L, checkpointInfo.getOffsetToCommit());
    Assertions.assertEquals(50L, checkpointInfo.getFetchOffset());
    Assertions.assertTrue(checkpointInfo.bounded(100L));
    Assertions.assertFalse(checkpointInfo.bounded(99L));
  }

  @Test
  public void testSet() {
    checkpointInfo.setOffsetToCommit(60L);
    checkpointInfo.setFetchOffset(70L);
    Assertions.assertEquals(60L, checkpointInfo.getOffsetToCommit());
    Assertions.assertEquals(70L, checkpointInfo.getFetchOffset());
  }
}
