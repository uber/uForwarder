package com.uber.data.kafka.datatransfer.controller.autoscalar;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** The type Auto scalar configuration. */
@ConfigurationProperties(prefix = "master.autoscalar")
public class AutoScalarConfiguration {
  private static final Duration DEFAULT_UP_SCALE_WINDOW_DURATION =
      Duration.ofMinutes(5); // 5 minutes
  private static final Duration DEFAULT_DOWN_SCALE_WINDOW_DURATION = Duration.ofDays(1); // 24 hours
  private static final Duration DEFAULT_HIBERNATE_WINDOW_DURATION = Duration.ofDays(3); // 72 hours
  private static final double DEFAULT_UP_SCALE_PERCENTILE = 0.5;
  private static final double DEFAULT_DOWN_SCALE_PERCENTILE = 0.99;
  private static final double DEFAULT_UP_SCALE_MIN_FACTOR = 1.2;
  private static final double DEFAULT_DOWN_SCALE_MIN_FACTOR = 0.5;
  private static final double DEFAULT_UP_SCALE_MAX_FACTOR = 2.0;
  private static final double DEFAULT_DOWN_SCALE_MAX_FACTOR = 0.8;
  private static final long DEFAULT_MESSAGES_PER_SECOND_PER_WORKER = 4000;
  private static final long DEFAULT_BYTES_PER_SECOND_PER_WORKER = 16 * 1024 * 1024; // 16MB/s

  // scale window duration for up scale
  private Duration upScaleWindowDuration = DEFAULT_UP_SCALE_WINDOW_DURATION;

  // scale window duration for down scale
  private Duration downScaleWindowDuration = DEFAULT_DOWN_SCALE_WINDOW_DURATION;

  // when window mature, if computed scale equals to zero, scale the job down to zero
  private Duration hibernateWindowDuration = DEFAULT_HIBERNATE_WINDOW_DURATION;

  // throughput window percentile for up scale
  private double upScalePercentile = DEFAULT_UP_SCALE_PERCENTILE;

  // throughput window percentile for down scale
  private double downScalePercentile = DEFAULT_DOWN_SCALE_PERCENTILE;

  // up scale minimal factor
  private double upScaleMinFactor = DEFAULT_UP_SCALE_MIN_FACTOR;

  // down scale minimal factor
  private double downScaleMinFactor = DEFAULT_DOWN_SCALE_MIN_FACTOR;

  // up scale maximum factor
  private double upScaleMaxFactor = DEFAULT_UP_SCALE_MAX_FACTOR;

  // down scale maximum factor
  private double downScaleMaxFactor = DEFAULT_DOWN_SCALE_MAX_FACTOR;

  // message per second per worker
  private long messagesPerSecPerWorker = DEFAULT_MESSAGES_PER_SECOND_PER_WORKER;

  // bytes per second per worker
  private long bytesPerSecPerWorker = DEFAULT_BYTES_PER_SECOND_PER_WORKER;

  // expiration time of through reported by worker
  private Duration throughputTTL = Duration.ofSeconds(60); // 60 seconds

  // expiration time of internal job status since last used
  // Autoscalar internal status including computed scale
  // will expire after given duration since job de-activated
  private Duration jobStatusTTL = Duration.ofMinutes(60); // 60 minutes

  // in dryRun mode, AutoScalar calculates but doesn't apply scale
  private boolean dryRun = true;

  // if to enable auto configuration
  private boolean enabled = false;

  // enable hibernating by scaling work load down to zero
  private boolean hibernatingEnabled = false;
  /**
   * Gets up scale window duration.
   *
   * @return the up scale window duration
   */
  public Duration getUpScaleWindowDuration() {
    return upScaleWindowDuration;
  }

  /**
   * Sets up scale window duration
   *
   * @param upScaleWindowDuration the up scale window minutes
   */
  public void setUpScaleWindowDuration(Duration upScaleWindowDuration) {
    this.upScaleWindowDuration = upScaleWindowDuration;
  }

  /**
   * Gets down scale window duration.
   *
   * @return the down scale window duration
   */
  public Duration getDownScaleWindowDuration() {
    return downScaleWindowDuration;
  }

  /**
   * Sets down scale window duration.
   *
   * @param downScaleWindowDuration the down scale window duration
   */
  public void setDownScaleWindowDuration(Duration downScaleWindowDuration) {
    this.downScaleWindowDuration = downScaleWindowDuration;
  }

  /**
   * Gets hibernate window duration.
   *
   * @return the hibernate window duration
   */
  public Duration getHibernateWindowDuration() {
    return hibernateWindowDuration;
  }

  /**
   * Sets hibernate window duration.
   *
   * @param hibernateWindowDuration the hibernate window duration
   */
  public void setHibernateWindowDuration(Duration hibernateWindowDuration) {
    this.hibernateWindowDuration = hibernateWindowDuration;
  }

  /**
   * Gets up scale percentile.
   *
   * @return the up scale percentile
   */
  public double getUpScalePercentile() {
    return upScalePercentile;
  }

  /**
   * Sets up scale percentile.
   *
   * @param upScalePercentile the up scale percentile
   */
  public void setUpScalePercentile(double upScalePercentile) {
    validateRange(upScalePercentile, 0.0, 1.0, "upScalePercentile");
    this.upScalePercentile = upScalePercentile;
  }

  /**
   * Gets down scale percentile.
   *
   * @return the down scale percentile
   */
  public double getDownScalePercentile() {
    return downScalePercentile;
  }

  /**
   * Sets down scale percentile.
   *
   * @param downScalePercentile the down scale percentile
   */
  public void setDownScalePercentile(double downScalePercentile) {
    validateRange(downScalePercentile, 0.0, 1.0, "downScalePercentile");
    this.downScalePercentile = downScalePercentile;
  }

  /**
   * Gets up scale min percent.
   *
   * @return the up scale min percent
   */
  public double getUpScaleMinFactor() {
    return upScaleMinFactor;
  }

  /**
   * Sets up scale min percent.
   *
   * @param upScaleMinFactor the up scale min percent
   */
  public void setUpScaleMinFactor(double upScaleMinFactor) {
    validateRange(upScaleMinFactor, 1.0, Double.MAX_VALUE, "upScaleMinFactor");
    this.upScaleMinFactor = upScaleMinFactor;
  }

  /**
   * Gets down scale min percent.
   *
   * @return the down scale min percent
   */
  public double getDownScaleMinFactor() {
    return downScaleMinFactor;
  }

  /**
   * Sets down scale min percent.
   *
   * @param downScaleMinFactor the down scale min percent
   */
  public void setDownScaleMinFactor(double downScaleMinFactor) {
    validateRange(downScaleMinFactor, 0.0, 1.0, "downScaleMinFactor");
    this.downScaleMinFactor = downScaleMinFactor;
  }

  /**
   * Gets up scale max percent.
   *
   * @return the up scale max percent
   */
  public double getUpScaleMaxFactor() {
    return upScaleMaxFactor;
  }

  /**
   * Sets up scale max percent.
   *
   * @param upScaleMaxFactor the up scale max percent
   */
  public void setUpScaleMaxFactor(double upScaleMaxFactor) {
    validateRange(upScaleMaxFactor, 1.0, Double.MAX_VALUE, "upScaleMaxFactor");
    this.upScaleMaxFactor = upScaleMaxFactor;
  }

  /**
   * Gets down scale max percent.
   *
   * @return the down scale max percent
   */
  public double getDownScaleMaxFactor() {
    return downScaleMaxFactor;
  }

  /**
   * Sets down scale max percent.
   *
   * @param downScaleMaxFactor the down scale max percent
   */
  public void setDownScaleMaxFactor(double downScaleMaxFactor) {
    validateRange(downScaleMaxFactor, 0.0, 1.0, "downScaleMaxFactor");
    this.downScaleMaxFactor = downScaleMaxFactor;
  }

  /**
   * Gets throughput ttl.
   *
   * @return the throughput ttl
   */
  public Duration getThroughputTTL() {
    return throughputTTL;
  }

  /**
   * Sets throughput ttl.
   *
   * @param throughputTTL the throughput ttl
   */
  public void setThroughputTTL(Duration throughputTTL) {
    this.throughputTTL = throughputTTL;
  }

  /**
   * Gets status store ttl.
   *
   * @return the status store ttl
   */
  public Duration getJobStatusTTL() {
    return jobStatusTTL;
  }

  /**
   * Sets status store ttl.
   *
   * @param jobStatusTTL the status store ttl
   */
  public void setJobStatusTTL(Duration jobStatusTTL) {
    this.jobStatusTTL = jobStatusTTL;
  }

  /**
   * Is enabled boolean.
   *
   * @return the boolean
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Sets enabled.
   *
   * @param enabled the enabled
   */
  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  /**
   * Is dryRun enabled.
   *
   * @return the boolean
   */
  public boolean isDryRun() {
    return dryRun;
  }

  /**
   * Sets dryRun mode
   *
   * @param dryRun the dry run
   */
  public void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  /**
   * Gets messages per sec per worker.
   *
   * @return the messages per sec per worker
   */
  public long getMessagesPerSecPerWorker() {
    return messagesPerSecPerWorker;
  }

  /**
   * Sets messages per sec per worker.
   *
   * @param messagesPerSecPerWorker the messages per sec per worker
   */
  public void setMessagesPerSecPerWorker(long messagesPerSecPerWorker) {
    this.messagesPerSecPerWorker = messagesPerSecPerWorker;
  }

  /**
   * Gets bytes per sec per worker.
   *
   * @return the bytes per sec per worker
   */
  public long getBytesPerSecPerWorker() {
    return bytesPerSecPerWorker;
  }

  /**
   * Sets bytes per sec per worker.
   *
   * @param bytesPerSecPerWorker the bytes per sec per worker
   */
  public void setBytesPerSecPerWorker(long bytesPerSecPerWorker) {
    this.bytesPerSecPerWorker = bytesPerSecPerWorker;
  }

  /**
   * Is hibernating enabled boolean.
   *
   * @return the boolean
   */
  public boolean isHibernatingEnabled() {
    return hibernatingEnabled;
  }

  /**
   * Sets hibernating enabled.
   *
   * @param hibernatingEnabled the hibernating enabled
   */
  public void setHibernatingEnabled(boolean hibernatingEnabled) {
    this.hibernatingEnabled = hibernatingEnabled;
  }

  private static void validateRange(double value, double min, double max, String name) {
    if (value < min) {
      throw new IllegalArgumentException(
          String.format("%s(%f) should not be smaller than %f", name, value, min));
    }

    if (value > max) {
      throw new IllegalArgumentException(
          String.format("%s(%f) should not be greater than %f", name, value, max));
    }
  }
}
