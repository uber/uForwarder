package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.codahale.metrics.Snapshot;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LatencyTrackerTest {
  private static final int maxInboundMessages = 1000;
  private static final int maxCommitSkew = 10000;
  private TestUtils.TestTicker ticker;

  private PipelineStateManager pipelineStateManager;
  private LatencyTracker latencyTracker;

  private FlowControl flowControl;
  private Map<Long, Job> runningJobMap;

  @BeforeEach
  public void setUp() {
    ticker = new TestUtils.TestTicker();
    runningJobMap = Collections.singletonMap(0L, Job.getDefaultInstance());
    flowControl = FlowControl.newBuilder().setMessagesPerSec(1000).build();
    pipelineStateManager = Mockito.mock(PipelineStateManager.class);
    Mockito.when(pipelineStateManager.getFlowControl()).thenReturn(flowControl);
    Mockito.when(pipelineStateManager.getExpectedRunningJobMap()).thenReturn(runningJobMap);

    latencyTracker = new LatencyTracker(maxInboundMessages, maxCommitSkew, ticker);
    latencyTracker.setPipelineStateManager(pipelineStateManager);
  }

  @Test
  public void testLatencyTrackerEmptyStats() {
    LatencyTracker latencyTracker = new LatencyTracker(maxInboundMessages, maxCommitSkew);
    latencyTracker.setPipelineStateManager(pipelineStateManager);
    LatencyTracker.Stats stats = latencyTracker.getStats();
    Assertions.assertFalse(stats.isMature());
  }

  @Test
  public void testGetStatsWithOneStats() {
    LatencyTracker.Stats stats = null;
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();

    for (int i = 0; i < 1000; ++i) {
      spans.add(latencyTracker.startSpan());
    }

    for (LatencyTracker.LatencySpan span : spans) {
      ticker.add(Duration.ofNanos(1));
      span.complete();
      stats = latencyTracker.getStats();
    }

    ticker.add(Duration.ofSeconds(11));
    stats = latencyTracker.getStats();
    Assertions.assertEquals(false, stats.isMature());
    Assertions.assertEquals(500, stats.getMedian(), 1);
    Assertions.assertEquals(1000, stats.getMax(), 1);
    Assertions.assertEquals(false, stats.isMedianLatencyHigh());
    Assertions.assertEquals(false, stats.isMaxLatencyHigh());
  }

  @Test
  public void testGetStatsWithZeroThroughput() {
    Mockito.when(pipelineStateManager.getFlowControl())
        .thenReturn(FlowControl.getDefaultInstance());
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();
    for (int i = 0; i < 1000; ++i) {
      spans.add(latencyTracker.startSpan());
    }

    for (LatencyTracker.LatencySpan span : spans) {
      // simulate long latency
      ticker.add(Duration.ofDays(1));
      span.complete();
    }

    LatencyTracker.Stats stats = latencyTracker.getStats();
    Assertions.assertFalse(stats.isMature());
    Assertions.assertEquals(0.0, percentDiff(stats.getMedian(), TimeUnit.DAYS.toNanos(500)), 0.01d);
    Assertions.assertEquals(0.0, percentDiff(stats.getMax(), TimeUnit.DAYS.toNanos(1000)), 0.01d);
    Assertions.assertEquals(false, stats.isMedianLatencyHigh());
    Assertions.assertEquals(false, stats.isMaxLatencyHigh());
  }

  @Test
  public void testGetStatsWithZeroPartition() {
    Mockito.when(pipelineStateManager.getExpectedRunningJobMap())
        .thenReturn(Collections.emptyMap());
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();
    for (int i = 0; i < 1000; ++i) {
      spans.add(latencyTracker.startSpan());
    }

    for (LatencyTracker.LatencySpan span : spans) {
      // simulate long latency
      ticker.add(Duration.ofDays(1));
      span.complete();
    }

    LatencyTracker.Stats stats = latencyTracker.getStats();
    Assertions.assertEquals(false, stats.isMature());
    Assertions.assertEquals(0.0, percentDiff(stats.getMedian(), TimeUnit.DAYS.toNanos(500)), 0.01d);
    Assertions.assertEquals(0.0, percentDiff(stats.getMax(), TimeUnit.DAYS.toNanos(1000)), 0.01d);
    Assertions.assertEquals(false, stats.isMedianLatencyHigh());
    Assertions.assertEquals(false, stats.isMaxLatencyHigh());
  }

  @Test
  public void testGetSnapshot() {
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

    Snapshot snapshot =
        latencyTracker.getLatencySnapshot(
            spans.toArray(new LatencyTracker.LatencySpan[0]), ticker.read());
    Assertions.assertEquals(450, snapshot.getMedian(), 1);
    Assertions.assertEquals(891, snapshot.get99thPercentile(), 1);
    Assertions.assertEquals(900, snapshot.getMax(), 1);
  }

  @Test
  public void testGetSnapshotWithIncompleteSpans() {
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();

    for (int i = 0; i < 1000; ++i) {
      spans.add(latencyTracker.startSpan());
    }
    ticker.add(Duration.ofNanos(1000));
    Snapshot snapshot =
        latencyTracker.getLatencySnapshot(
            spans.toArray(new LatencyTracker.LatencySpan[0]), ticker.read());
    Assertions.assertEquals(1000, snapshot.getMedian(), 1);
    Assertions.assertEquals(1000, snapshot.getMax(), 1);
  }

  @Test
  public void testNewStatsWithZeroThroughput() {
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();
    for (int i = 0; i < 1000; ++i) {
      spans.add(latencyTracker.startSpan());
    }
    for (LatencyTracker.LatencySpan span : spans) {
      ticker.add(Duration.ofNanos(1));
      span.complete();
    }

    Mockito.when(pipelineStateManager.getFlowControl())
        .thenReturn(FlowControl.getDefaultInstance());

    LatencyTracker.Stats stats = latencyTracker.getStats();
    Assertions.assertEquals(false, stats.isMature());
    Assertions.assertEquals(500, stats.getMedian());
    Assertions.assertEquals(1000, stats.getMax());
    Assertions.assertEquals(false, stats.isMaxLatencyHigh());
    Assertions.assertEquals(false, stats.isMedianLatencyHigh());
  }

  @Test
  public void testNewStatsWithHighThroughput() {
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();
    for (int i = 0; i < 1000; ++i) {
      spans.add(latencyTracker.startSpan());
    }
    for (LatencyTracker.LatencySpan span : spans) {
      ticker.add(Duration.ofNanos(1));
      span.complete();
    }

    Mockito.when(pipelineStateManager.getFlowControl())
        .thenReturn(FlowControl.newBuilder().setMessagesPerSec(Double.MAX_VALUE).build());
    LatencyTracker.Stats stats = latencyTracker.getStats();
    Assertions.assertEquals(false, stats.isMature());
    Assertions.assertEquals(500, stats.getMedian());
    Assertions.assertEquals(1000, stats.getMax());
    Assertions.assertEquals(true, stats.isMaxLatencyHigh());
    Assertions.assertEquals(true, stats.isMedianLatencyHigh());
  }

  @Test
  public void testGetMaxLatency_WithLargeThroughput() {
    long maxLatency = LatencyTracker.getMaxLatency(10000, 1000);
    Assertions.assertEquals((long) (0.1 * TimeUnit.SECONDS.toNanos(1)), maxLatency);
  }

  @Test
  public void testGetMaxLatency_WithLargeConcurrencyLimit() {
    long maxLatency = LatencyTracker.getMaxLatency(4.9e-324, Integer.MAX_VALUE);
    Assertions.assertEquals(Long.MAX_VALUE, maxLatency);
  }

  private double percentDiff(double v1, double v2) {
    return (v1 - v2) / v2;
  }
}
