package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.google.common.math.DoubleMath;
import com.uber.data.kafka.datatransfer.worker.common.Configurable;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Tracker and statistics for latency. the class support analysis of request latency and provide
 * latency statistics such as median and max latency in a sliding window of fixed size
 */
public class LatencyTracker implements Configurable {
  private static final int LATENCY_RESERVOIR_SIZE = 10000;
  // default interval between samples, reduce to improve precision at cost of more CPU
  private static final int DEFAULT_SAMPLE_INTERVAL_SECONDS = 10;
  private static final double EPSILON = 0.000001;
  private final Linger linger;
  private final LatencySpan[] latencySpans;
  private final AtomicInteger index = new AtomicInteger(0);
  private final Ticker ticker;
  private final int maxInboundMessages;
  private final int maxCommitSkew;
  private final AtomicReference<Sample> sampleRef;
  @Nullable private volatile PipelineStateManager pipelineStateManager;

  /** Instantiates a new Latency tracker with default configuration */
  public LatencyTracker(int maxInboundMessages, int maxCommitSkew) {
    this(maxInboundMessages, maxCommitSkew, Ticker.systemTicker(), DEFAULT_SAMPLE_INTERVAL_SECONDS);
  }

  /**
   * Instantiates a new Latency tracker.
   *
   * @param ticker the ticker
   * @param sampleIntervalSeconds the interval between each sample in seconds
   */
  public LatencyTracker(
      int maxInboundMessages, int maxCommitSkew, Ticker ticker, int sampleIntervalSeconds) {
    this.maxInboundMessages = maxInboundMessages;
    this.maxCommitSkew = maxCommitSkew;
    this.latencySpans = new LatencySpan[LATENCY_RESERVOIR_SIZE];
    this.ticker = ticker;
    this.linger = new Linger(ticker, TimeUnit.SECONDS.toNanos(sampleIntervalSeconds));
    this.sampleRef = new AtomicReference<>();
  }

  public LatencySpan startSpan() {
    int id = index.get();
    while (!index.compareAndSet(id, (id + 1) % LATENCY_RESERVOIR_SIZE)) {
      id = index.get();
    }
    LatencySpan ret = new LatencySpan(ticker.read());
    latencySpans[id] = ret;
    return ret;
  }

  /** Gets a sample of latency statistics */
  public Sample getSample() {
    if (linger.tickIfNecessary()) {
      // refresh sample
      Snapshot snapshot = getLatencySnapshot(latencySpans, ticker.read());
      sampleRef.set(newSample(snapshot, messagesPerSec(), maxInboundMessages, maxCommitSkew));
    }
    Sample ret = sampleRef.get();
    if (ret == null) {
      return Sample.EMPTY_SAMPLE;
    } else {
      return ret;
    }
  }

  @Override
  public void setPipelineStateManager(PipelineStateManager pipelineStateManager) {
    this.pipelineStateManager = pipelineStateManager;
  }

  /**
   * Gets latency of all completed requests as a snapshot
   *
   * @param latencySpans the latency spans
   * @return latency snapshot
   */
  @VisibleForTesting
  protected static Snapshot getLatencySnapshot(LatencySpan[] latencySpans, long nowNano) {
    List<Long> latencies = new ArrayList<>();
    for (final LatencySpan latencySpan : latencySpans) {
      if (latencySpan != null) {
        long latency = latencySpan.duration(nowNano);
        if (latency != 0) {
          latencies.add(latency);
        }
      }
    }
    return new UniformSnapshot(latencies);
  }

  /**
   * Creates a new statistics sample from a snapshot of latencies Includes latency thresholds
   * computed from throughput and concurrency limits
   *
   * @param snapshot the snapshot
   * @param messagesPerSec the messages per sec
   * @param maxInboundMessages the max inbound messages
   * @param maxCommitSkew the max commit skew
   * @return the sample
   */
  @VisibleForTesting
  protected static Sample newSample(
      Snapshot snapshot, double messagesPerSec, int maxInboundMessages, int maxCommitSkew) {
    long maxMedianLatency = getMaxLatency(messagesPerSec, maxInboundMessages);
    long maxMaxLatency = getMaxLatency(messagesPerSec, maxCommitSkew);
    return new Sample(
        snapshot.size(),
        (long) snapshot.getMedian(),
        maxMedianLatency,
        snapshot.getMax(),
        maxMaxLatency);
  }

  private double messagesPerSec() {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    double messagesPerSec = pipelineStateManager.getFlowControl().getMessagesPerSec();
    int nPartitions = pipelineStateManager.getExpectedRunningJobMap().size();
    if (nPartitions == 0) {
      return 0.0;
    }
    return messagesPerSec / nPartitions;
  }

  /**
   * Estimates max latency in nanoseconds given concurrency limit and throughput use the little's
   * law
   *
   * @param messagesPerSec
   * @param concurrencyLimit
   * @return
   */
  private static long getMaxLatency(double messagesPerSec, int concurrencyLimit) {
    if (DoubleMath.fuzzyEquals(messagesPerSec, 0.0, EPSILON)) {
      return Long.MAX_VALUE;
    }

    long maxLatencyInSeconds = (long) (concurrencyLimit / (messagesPerSec));
    return TimeUnit.SECONDS.toNanos(maxLatencyInSeconds);
  }

  /** The type Sample. */
  public static class Sample {
    public static Sample EMPTY_SAMPLE = new Sample(0, 0, 0, 0, 0);
    private final int size;
    private final long median;
    private final long max;
    private final long maxMedian;
    private final long maxMax;

    /**
     * Instantiates a new Sample.
     *
     * @param size the size of completed requests in the sample
     * @param median the median
     * @param maxMedian the max median
     * @param max the max
     * @param maxMax the max max
     */
    Sample(int size, long median, long maxMedian, long max, long maxMax) {
      this.size = size;
      this.median = median;
      this.maxMedian = maxMedian;
      this.max = max;
      this.maxMax = maxMax;
    }

    /**
     * Gets number of events in the sample
     *
     * @return the int
     */
    public int size() {
      return size;
    }

    @VisibleForTesting
    protected long getMedian() {
      return median;
    }

    @VisibleForTesting
    protected long getMax() {
      return max;
    }

    /**
     * Gets if median latency is too high.
     *
     * @return boolean
     */
    public boolean isMedianLatencyHigh() {
      return median > maxMedian;
    }

    /**
     * Gets if max latency is too high
     *
     * @return boolean
     */
    public boolean isMaxLatencyHigh() {
      return max > maxMax;
    }
  }

  protected class Linger {
    private final long intervalNano;
    private final Ticker ticker;
    private AtomicLong lastTickNano = new AtomicLong(0);

    Linger(Ticker ticker, long intervalNano) {
      this.ticker = ticker;
      this.intervalNano = intervalNano;
    }

    /**
     * Tick if interval is larger than the intervalNano, and update the last tick time
     *
     * @return true if interval is larger than the intervalNano
     */
    public boolean tickIfNecessary() {
      long now = ticker.read();
      long oldTick = lastTickNano.get();
      if (now - oldTick > intervalNano) {
        if (lastTickNano.compareAndSet(oldTick, now)) {
          return true;
        }
      }
      return false;
    }
  }

  public class LatencySpan {
    private final long startNano;
    private volatile long endNano;

    private LatencySpan(long startNano) {
      this.startNano = startNano;
    }

    public void complete() {
      this.endNano = ticker.read();
    }

    private boolean isCompleted() {
      return endNano != 0;
    }

    /**
     * Gets the duration of the latency span in nanoseconds if span is completed, returns actual
     * latency otherwise returns pending latency (duration between start time and now)
     *
     * @return
     */
    private long duration(long nowNano) {
      if (isCompleted()) {
        return endNano - startNano;
      } else {
        return nowNano - startNano;
      }
    }
  }
}
