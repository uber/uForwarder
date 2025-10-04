package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

public class LatencyTrackerTest extends FievelTestBase {
  private static final double SAMPLE_RATIO = 1.0;

  @Test
  public void testLatencyTrackerEmptySample() {
    LatencyTracker latencyTracker = new LatencyTracker();
    Optional<LatencyTracker.Sample> sample = latencyTracker.updateRequestLatency(1000);
    Assert.assertTrue(sample.isEmpty());
  }

  @Test
  public void testLatencyTrackerWithOneSample() {
    LatencyTracker latencyTracker = new LatencyTracker(SAMPLE_RATIO);
    final AtomicInteger sampleCount = new AtomicInteger(0);
    final AtomicReference<LatencyTracker.Sample> refSample = new AtomicReference<>();
    for (int i = 0; i < 1000; ++i) {
      Optional<LatencyTracker.Sample> sample = latencyTracker.updateRequestLatency(i);
      if (!sample.isEmpty()) {
        sampleCount.getAndIncrement();
        refSample.compareAndSet(null, sample.get());
      }
    }

    Assert.assertEquals(1, sampleCount.get());
    Assert.assertEquals(500, refSample.get().getMedian(), 1);
    Assert.assertEquals(990, refSample.get().getP99Percentile(), 1);
  }
}
