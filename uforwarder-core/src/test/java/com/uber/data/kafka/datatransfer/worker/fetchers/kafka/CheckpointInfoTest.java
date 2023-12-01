package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CheckpointInfoTest extends FievelTestBase {
  private Job job;
  private CheckpointInfo checkpointInfo;

  @Before
  public void setUp() {
    job = Job.newBuilder().build();
    checkpointInfo = new CheckpointInfo(job, 50L, 100L);
  }

  @Test
  public void testGet() {
    Assert.assertEquals(job, checkpointInfo.getJob());
    Assert.assertEquals(50L, checkpointInfo.getStartingOffset());
    Assert.assertEquals(50L, checkpointInfo.getOffsetToCommit());
    Assert.assertEquals(50L, checkpointInfo.getFetchOffset());
    Assert.assertTrue(checkpointInfo.bounded(100L));
    Assert.assertFalse(checkpointInfo.bounded(99L));
  }

  @Test
  public void testSet() {
    checkpointInfo.setOffsetToCommit(60L);
    checkpointInfo.setFetchOffset(70L);
    Assert.assertEquals(60L, checkpointInfo.getOffsetToCommit());
    Assert.assertEquals(70L, checkpointInfo.getFetchOffset());
  }
}
