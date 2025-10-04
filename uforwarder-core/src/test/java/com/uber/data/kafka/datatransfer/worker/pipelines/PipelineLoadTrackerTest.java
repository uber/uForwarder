package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class PipelineLoadTrackerTest extends FievelTestBase {
  @Test
  public void testGetLoad() {
    PipelineLoadTracker tracker = PipelineLoadTracker.NOOP;
    Assert.assertTrue(tracker.getLoad().getCoreCpuUsage() == 0.0);
    Assert.assertTrue(tracker.getLoad().getCpuUsage() == 0.0);
  }
}
