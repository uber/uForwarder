package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.codahale.metrics.UniformSnapshot;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Tracker and statistics for latency. the class support analysis of request latency and provide
 * latency statistics such as median and max latency in a sliding window of fixed size
 */
public class LatencyTracker {
  private static final int LATENCY_RESERVOIR_SIZE = 10000;
  // minimum number of samples to consider the request latency histogram mature
  private static final int MIN_MATURE_REQUEST_LATENCY_SAMPLES = 100;
  // default interval between samples, reduce to improve precision at cost of more CPU
  private static final int DEFAULT_SAMPLE_INTERVAL_SECONDS = 10;
  private final Linger linger;
  private final AtomicReference<Histogram> requestLatencyHistogram;
  private final LatencySpan[] latencySpans;
  private final AtomicInteger index = new AtomicInteger(0);
  private final Ticker ticker;
  private @Nullable volatile Sample lastSample = null;

  /** Instantiates a new Latency tracker with default configuration */
  public LatencyTracker() {
    this(Ticker.systemTicker(), DEFAULT_SAMPLE_INTERVAL_SECONDS);
  }

  /**
   * Instantiates a new Latency tracker.
   *
   * @param ticker the ticker
   * @param sampleIntervalSeconds the interval between each sample in seconds
   */
  public LatencyTracker(Ticker ticker, int sampleIntervalSeconds) {
    this.latencySpans = new LatencySpan[LATENCY_RESERVOIR_SIZE];
    this.requestLatencyHistogram =
        new AtomicReference<>(new Histogram(new UniformReservoir(LATENCY_RESERVOIR_SIZE)));
    this.ticker = ticker;
    this.linger = new Linger(ticker, TimeUnit.SECONDS.toNanos(sampleIntervalSeconds));
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

  /**
   * Updates request latency and return on sample of latency statistics there could no valid sample
   * if the number of request is too small
   */
  public Optional<Sample> getSample() {
    if (linger.tickIfNecessary()) {
      // refresh sample
      Snapshot snapshot = getLatencySnapshot();
      long maxPendingRequestLatency = getMaxPendingRequestLatency();
      if (snapshot.size() >= MIN_MATURE_REQUEST_LATENCY_SAMPLES) {
        // max latency should be max latency of complected requests and pending requests
        long maxLatency = Math.max(snapshot.getMax(), maxPendingRequestLatency);
        lastSample = new Sample(snapshot.getMedian(), maxLatency);
      } else {
        lastSample = null;
      }
    }
    return Optional.ofNullable(lastSample);
  }

  /**
   * Gets latency of all completed requests as a snapshot
   *
   * @return
   */
  @VisibleForTesting
  protected Snapshot getLatencySnapshot() {
    List<Long> latencies = new ArrayList<>();
    for (final LatencySpan latencySpan : latencySpans) {
      if (latencySpan != null) {
        long latency = latencySpan.duration();
        if (latency != 0) {
          latencies.add(latency);
        }
      }
    }
    return new UniformSnapshot(latencies);
  }

  /**
   * Gets the max latency of pending requests latency of a pending request is between now and
   * request start time
   *
   * @return
   */
  @VisibleForTesting
  protected long getMaxPendingRequestLatency() {
    long nowNano = ticker.read();
    long ret = Long.MIN_VALUE;
    for (final LatencySpan latencySpan : latencySpans) {
      if (latencySpan != null) {
        if (!latencySpan.isCompleted()) {
          ret = Math.max(ret, nowNano - latencySpan.startNano);
        }
      }
    }
    return ret;
  }

  /** The type Sample. */
  public static class Sample {
    private double median;
    private double max;

    /**
     * Instantiates a new Sample.
     *
     * @param median the median latency
     * @param max the max latency
     */
    Sample(double median, double max) {
      this.median = median;
      this.max = max;
    }

    /**
     * Gets median latency.
     *
     * @return the median latency
     */
    public double getMedian() {
      return median;
    }

    /**
     * Gets max latency.
     *
     * @return the max latency
     */
    public double getMax() {
      return max;
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
     * Gets the start time of the request 0 if the request is not completed
     *
     * @return
     */
    private long duration() {
      if (isCompleted()) {
        return endNano - startNano;
      }
      return 0L;
    }
  }
}
