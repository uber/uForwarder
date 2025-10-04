package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class LatencyTrackerTest extends FievelTestBase {
  private static final int interval = 10;

  private static final int maxInboundMessages = 1000;
  private static final int maxCommitSkew = 10000;
  private TestUtils.TestTicker ticker;

  private PipelineStateManager pipelineStateManager;
  private LatencyTracker latencyTracker;

  private FlowControl flowControl;
  private Map<Long, Job> runningJobMap;

  @Before
  public void setUp() {
    ticker = new TestUtils.TestTicker();
    runningJobMap = Collections.singletonMap(0L, Job.getDefaultInstance());
    flowControl = FlowControl.newBuilder().setMessagesPerSec(1000).build();
    pipelineStateManager = Mockito.mock(PipelineStateManager.class);
    Mockito.when(pipelineStateManager.getFlowControl()).thenReturn(flowControl);
    Mockito.when(pipelineStateManager.getExpectedRunningJobMap()).thenReturn(runningJobMap);

    latencyTracker = new LatencyTracker(maxInboundMessages, maxCommitSkew, ticker, interval);
    latencyTracker.setPipelineStateManager(pipelineStateManager);
  }

  @Test
  public void testLatencyTrackerEmptySample() {
    LatencyTracker latencyTracker = new LatencyTracker(maxInboundMessages, maxCommitSkew);
    latencyTracker.setPipelineStateManager(pipelineStateManager);
    LatencyTracker.Sample sample = latencyTracker.getSample();
    Assert.assertEquals(sample.size(), 0);
  }

  @Test
  public void testGetSampleWithOneSample() {
    LatencyTracker.Sample sample = null;
    List<LatencyTracker.LatencySpan> spans = new ArrayList<>();

    for (int i = 0; i < 1000; ++i) {
      spans.add(latencyTracker.startSpan());
    }

    for (LatencyTracker.LatencySpan span : spans) {
      ticker.add(Duration.ofNanos(1));
      span.complete();
      sample = latencyTracker.getSample();
    }

    Assert.assertEquals(0, sample.size());
    ticker.add(Duration.ofSeconds(11));
    sample = latencyTracker.getSample();
    Assert.assertEquals(1000, sample.size());
    Assert.assertEquals(500, sample.getMedian(), 1);
    Assert.assertEquals(1000, sample.getMax(), 1);
    Assert.assertEquals(false, sample.isMedianLatencyHigh());
    Assert.assertEquals(false, sample.isMaxLatencyHigh());
  }

  @Test
  public void testGetSampleWithZeroThroughput() {
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

    LatencyTracker.Sample sample = latencyTracker.getSample();
    Assert.assertEquals(1000, sample.size());
    Assert.assertEquals(0.0, percentDiff(sample.getMedian(), TimeUnit.DAYS.toNanos(500)), 0.01d);
    Assert.assertEquals(0.0, percentDiff(sample.getMax(), TimeUnit.DAYS.toNanos(1000)), 0.01d);
    Assert.assertEquals(false, sample.isMedianLatencyHigh());
    Assert.assertEquals(false, sample.isMaxLatencyHigh());
  }

  @Test
  public void testGetSampleWithZeroPartition() {
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

    LatencyTracker.Sample sample = latencyTracker.getSample();
    Assert.assertEquals(1000, sample.size());
    Assert.assertEquals(0.0, percentDiff(sample.getMedian(), TimeUnit.DAYS.toNanos(500)), 0.01d);
    Assert.assertEquals(0.0, percentDiff(sample.getMax(), TimeUnit.DAYS.toNanos(1000)), 0.01d);
    Assert.assertEquals(false, sample.isMedianLatencyHigh());
    Assert.assertEquals(false, sample.isMaxLatencyHigh());
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
    Assert.assertEquals(450, snapshot.getMedian(), 1);
    Assert.assertEquals(891, snapshot.get99thPercentile(), 1);
    Assert.assertEquals(900, snapshot.getMax(), 1);
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
    Assert.assertEquals(1000, snapshot.getMedian(), 1);
    Assert.assertEquals(1000, snapshot.getMax(), 1);
  }

  @Test
  public void testNewSample() {
    long[] values = java.util.stream.LongStream.rangeClosed(1L, 1000L).toArray();
    Snapshot snapshot = new UniformSnapshot(values);
    LatencyTracker.Sample sample = LatencyTracker.newSample(snapshot, 1000, 1000, 10000);
    Assert.assertEquals(1000, sample.size());
    Assert.assertEquals(500, sample.getMedian());
    Assert.assertEquals(1000, sample.getMax());
    Assert.assertEquals(false, sample.isMaxLatencyHigh());
    Assert.assertEquals(false, sample.isMedianLatencyHigh());
  }

  @Test
  public void testNewSampleWithZeroThroughput() {
    long[] values = java.util.stream.LongStream.rangeClosed(1L, 1000L).toArray();
    Snapshot snapshot = new UniformSnapshot(values);
    LatencyTracker.Sample sample = LatencyTracker.newSample(snapshot, 0, 1000, 10000);
    Assert.assertEquals(1000, sample.size());
    Assert.assertEquals(500, sample.getMedian());
    Assert.assertEquals(1000, sample.getMax());
    Assert.assertEquals(false, sample.isMaxLatencyHigh());
    Assert.assertEquals(false, sample.isMedianLatencyHigh());
  }

  @Test
  public void testNewSampleWithHighThroughput() {
    long[] values = java.util.stream.LongStream.rangeClosed(1L, 1000L).toArray();
    Snapshot snapshot = new UniformSnapshot(values);
    LatencyTracker.Sample sample =
        LatencyTracker.newSample(snapshot, Integer.MAX_VALUE, 1000, 10000);
    Assert.assertEquals(1000, sample.size());
    Assert.assertEquals(500, sample.getMedian());
    Assert.assertEquals(1000, sample.getMax());
    Assert.assertEquals(true, sample.isMaxLatencyHigh());
    Assert.assertEquals(true, sample.isMedianLatencyHigh());
  }

  private double percentDiff(double v1, double v2) {
    return (v1 - v2) / v2;
  }
}
