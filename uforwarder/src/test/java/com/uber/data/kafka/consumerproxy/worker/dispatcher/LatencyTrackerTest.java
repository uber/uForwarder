package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.codahale.metrics.Snapshot;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class LatencyTrackerTest extends FievelTestBase {
  private static final int interval = 10;
  private static final TestUtils.TestTicker ticker = new TestUtils.TestTicker();

  @Test
  public void testLatencyTrackerEmptySample() {
    LatencyTracker latencyTracker = new LatencyTracker();
    Optional<LatencyTracker.Sample> sample = latencyTracker.getSample();
    Assert.assertTrue(sample.isEmpty());
  }

  @Test
  public void testLatencyTrackerWithOneSample() {
    LatencyTracker latencyTracker = new LatencyTracker(ticker, interval);
    Optional<LatencyTracker.Sample> sample = Optional.empty();
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();

    for (int i = 0; i < 1000; ++i) {
      spans.add(latencyTracker.startSpan());
    }

    for (LatencyTracker.LatencySpan span : spans) {
      ticker.add(Duration.ofNanos(1));
      span.complete();
      sample = latencyTracker.getSample();
    }

    Assert.assertFalse(sample.isPresent());
    ticker.add(Duration.ofSeconds(11));
    sample = latencyTracker.getSample();
    Assert.assertTrue(sample.isPresent());
    Assert.assertEquals(500, sample.get().getMedian(), 1);
    Assert.assertEquals(1000, sample.get().getMax(), 1);
  }

  @Test
  public void testGetMaxPendingRequestLatency() {
    LatencyTracker latencyTracker = new LatencyTracker(ticker, interval);
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();

    for (int i = 0; i < 1000; ++i) {
      LatencyTracker.LatencySpan span = latencyTracker.startSpan();
      if (i < 900) {
        spans.add(span);
      }
    }

    for (LatencyTracker.LatencySpan span : spans) {
      ticker.add(Duration.ofNanos(1));
      span.complete();
    }

    Assert.assertEquals(900, latencyTracker.getMaxPendingRequestLatency());
  }

  @Test
  public void testGetSnapshot() {
    LatencyTracker latencyTracker = new LatencyTracker(ticker, interval);
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();

    for (int i = 0; i < 1000; ++i) {
      if (i < 900) {
        spans.add(latencyTracker.startSpan());
      }
    }

    for (LatencyTracker.LatencySpan span : spans) {
      ticker.add(Duration.ofNanos(1));
      span.complete();
    }

    Snapshot snapshot = latencyTracker.getLatencySnapshot();
    Assert.assertEquals(450, snapshot.getMedian(), 1);
    Assert.assertEquals(891, snapshot.get99thPercentile(), 1);
    Assert.assertEquals(900, snapshot.getMax(), 1);
  }
}
