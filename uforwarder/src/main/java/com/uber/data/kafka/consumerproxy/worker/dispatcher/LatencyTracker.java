package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Snapshot;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Tracker and statistics for latency. the class support analysis of request latency and provide
 * latency statistics such as median and p99 latency in a sliding time window
 */
public class LatencyTracker {
  private static final int REQUEST_LATENCY_WINDOW_SECONDS = 60 * 10;
  // minimum number of samples to consider the request latency histogram mature
  private static final int MIN_MATURE_REQUEST_LATENCY_SAMPLES = 1000;
  // default latency sample to request ratio, increase to improve precision at cost of more CPU
  private static final double DEFAULT_SAMPLE_RATIO = 0.01;
  private final double sampleRatio;
  private final Histogram requestLatencyHistogram;
  private @Nullable volatile Sample lastSample = null;

  /** Instantiates a new Latency tracker with default configuration */
  public LatencyTracker() {
    this(DEFAULT_SAMPLE_RATIO);
  }

  /**
   * Instantiates a new Latency tracker.
   *
   * @param sampleRatio the ratio of statistics sample to requests
   */
  public LatencyTracker(double sampleRatio) {
    this.requestLatencyHistogram =
        new Histogram(
            new SlidingTimeWindowArrayReservoir(REQUEST_LATENCY_WINDOW_SECONDS, TimeUnit.SECONDS));
    this.sampleRatio = sampleRatio;
  }

  /**
   * Update request latency and return on sample of latency statistics there could no valid sample
   * if the number of request is too small
   *
   * @param durationNanos the duration nanos of a request
   */
  public Optional<Sample> updateRequestLatency(long durationNanos) {
    requestLatencyHistogram.update(durationNanos);
    if (Math.random() < sampleRatio) {
      // refresh sample
      Snapshot snapshot = requestLatencyHistogram.getSnapshot();
      if (snapshot.size() >= MIN_MATURE_REQUEST_LATENCY_SAMPLES) {
        lastSample = new Sample(snapshot.getMedian(), snapshot.get99thPercentile());
      } else {
        lastSample = null;
      }
    }
    final Sample finalSample = lastSample;
    if (finalSample != null) {
      return Optional.of(finalSample);
    } else {
      return Optional.empty();
    }
  }

  /** The type Sample. */
  public static class Sample {
    private double median;
    private double p99Percentile;

    /**
     * Instantiates a new Sample.
     *
     * @param median the median latency
     * @param p99Percentile the p99 percentile latency
     */
    Sample(double median, double p99Percentile) {
      this.median = median;
      this.p99Percentile = p99Percentile;
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
     * Gets p99 percentile latency.
     *
     * @return the p99 percentile latency
     */
    public double getP99Percentile() {
      return p99Percentile;
    }
  }
}
