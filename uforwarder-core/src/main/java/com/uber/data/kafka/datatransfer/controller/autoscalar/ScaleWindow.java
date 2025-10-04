package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.uber.data.kafka.datatransfer.WindowSnapshot;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * ThroughputWindow aggregates samples of throughput into bounded window
 *
 * <p>The system works as below: 1. job scale reported by worker stored in {@link
 * JobThroughputMonitor} 2. AutoScalar aggregates job throughput into job group throughput 3.
 * AutoScalar coverts job group throughput into scale 4. AutoScalar store scale stored in {@link
 * ScaleWindow} 5. AutoScalar get certain percentile of job group scale from {@link ScaleWindow} as
 * load indicator of job
 */
public class ScaleWindow {
  private final Supplier<Long> windowDurationSupplier;
  // minimal samples of the window to mature
  private final int minSamples;
  // time the window started in nano
  private final long startTimeNano;
  private final BoundedWindow boundedWindow;
  private final Ticker ticker;
  private final long jitterNano;
  private int nSamples;

  private ScaleWindow(Builder builder) {
    this.windowDurationSupplier = builder.windowDurationSupplier;
    this.minSamples = builder.minSamples;
    this.ticker = builder.ticker;
    this.jitterNano = (long) (builder.jitter * windowDurationSupplier.get());
    this.startTimeNano = ticker.read();
    this.boundedWindow = new BoundedWindow(builder.nBuckets, builder.minScale, builder.maxScale);
  }

  /**
   * Adds scale into the window
   *
   * @param scale the scale
   */
  public void add(double scale) {
    boundedWindow.add(scale);
    nSamples++;
  }

  /**
   * Gets scale by percentile.
   *
   * @param percentile the percentile
   * @return the scale by percentile
   */
  public double getByPercentile(double percentile) {
    return boundedWindow.getByPercentile(percentile);
  }

  /**
   * Tests if the window is mature
   *
   * @return the boolean
   */
  public boolean isMature() {
    long windowDurationNano = windowDurationSupplier.get();
    return nSamples >= minSamples
        && (ticker.read() - startTimeNano) > (windowDurationNano - jitterNano);
  }

  public WindowSnapshot snapshot() {
    return WindowSnapshot.newBuilder()
        .setSizeInSeconds(TimeUnit.NANOSECONDS.toSeconds(ticker.read() - startTimeNano))
        .setMinSizeInSeconds(
            TimeUnit.NANOSECONDS.toSeconds(windowDurationSupplier.get() - jitterNano))
        .setSizeInSamples(nSamples)
        .setMinSizeInSamples(minSamples)
        .build();
  }

  /**
   * New builder.
   *
   * @return the builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  private static class BoundedWindow {
    private final double[] buckets;
    private final double bucketWidth;
    private final double minValue;
    private int nSamples;

    BoundedWindow(int nBuckets, double minValue, double maxValue) {
      this.buckets = new double[nBuckets];
      this.bucketWidth = (maxValue - minValue) / nBuckets;
      this.minValue = minValue;
    }

    /**
     * Adds value into the window
     *
     * @param value the throughput
     */
    public void add(double value) {
      int index = (int) Math.ceil((value - minValue) / bucketWidth);
      index = Math.min(buckets.length - 1, Math.max(0, index));
      buckets[index]++;
      nSamples++;
    }

    /**
     * Gets value by percentile.
     *
     * @param percentile the percentile
     * @return the value by percentile
     */
    public double getByPercentile(double percentile) {
      percentile = Math.max(0, Math.min(1, percentile));
      int pivot = (int) (percentile * nSamples);
      int sum = 0;
      int index = 0;
      for (; index < buckets.length; ++index) {
        sum += buckets[index];
        if (sum >= pivot) {
          break;
        }
      }
      return minValue + index * bucketWidth;
    }
  }

  /** The type Builder. */
  public static class Builder {
    private static final int DEFAULT_BUCKETS = 100;
    private static final int DEFAULT_MIN_SAMPLES = 10;
    private Supplier<Long> windowDurationSupplier = () -> TimeUnit.HOURS.toNanos(1);
    private int minSamples = DEFAULT_MIN_SAMPLES;
    private double minScale;
    private double maxScale;
    private int nBuckets = DEFAULT_BUCKETS;
    private Ticker ticker = Ticker.systemTicker();
    private double jitter = 0.0;

    /**
     * Sets number of buckets
     *
     * @param nBuckets the n buckets
     * @return the builder
     */
    public Builder withNBuckets(int nBuckets) {
      if (nBuckets <= 0) {
        throw new IllegalArgumentException(String.format("Invalid nBuckets=%d", nBuckets));
      }
      this.nBuckets = nBuckets;
      return this;
    }

    /**
     * Sets minimal samples of a matured window
     *
     * @param minSamples minimal samples of a matured window
     * @return the builder
     */
    public Builder withMinSamples(int minSamples) {
      if (minSamples <= 0) {
        throw new IllegalArgumentException(String.format("Invalid minSamples=%d", minSamples));
      }
      this.minSamples = minSamples;
      return this;
    }

    /**
     * Sets window duration supplier
     *
     * @param windowDurationSupplier
     * @return the builder
     */
    public Builder withWindowDurationSupplier(Supplier<Long> windowDurationSupplier) {
      this.windowDurationSupplier = windowDurationSupplier;
      return this;
    }

    /**
     * Sets ticker
     *
     * @param ticker the ticker
     * @return the builder
     */
    public Builder withTicker(Ticker ticker) {
      this.ticker = ticker;
      return this;
    }

    /**
     * Build ThroughputWindow
     *
     * @param minScale the min scale, when actual sample is smaller than min scale, it will be up
     *     scale to min scale
     * @param maxScale the max scale, when actual sample is lager than max scale, it will be down
     *     scale to max scale
     * @param jitter percentage of window duration that window start time should subtract from
     * @return the throughput window
     */
    public ScaleWindow build(double minScale, double maxScale, double jitter) {
      Preconditions.checkArgument(jitter < 1.0, "jitter must be smaller than 1");
      Preconditions.checkArgument(
          minScale <= maxScale, "minScale must be smaller or equals to maxScale");
      this.minScale = minScale;
      this.maxScale = maxScale;
      this.jitter = jitter;
      return new ScaleWindow(this);
    }
  }
}
