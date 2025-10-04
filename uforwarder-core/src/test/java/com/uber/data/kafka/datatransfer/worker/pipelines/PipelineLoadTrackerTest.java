package com.uber.data.kafka.datatransfer.worker.pipelines;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PipelineLoadTrackerTest {
  @Test
  public void testGetLoad() {
    PipelineLoadTracker tracker = PipelineLoadTracker.NOOP;
    Assertions.assertTrue(tracker.getLoad().getCoreCpuUsage() == 0.0);
    Assertions.assertTrue(tracker.getLoad().getCpuUsage() == 0.0);
  }
}
