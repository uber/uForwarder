package com.uber.data.kafka.datatransfer.controller.autoscalar;

import java.time.Duration;

/**
 * {@code ScaleWindowManager} is responsible for managing the window durations used in the
 * auto-scaling logic for Kafka job groups. It encapsulates the durations for down-scaling,
 * up-scaling, and hibernation windows, which are used to control the responsiveness and stability
 * of the auto-scaling mechanism.
 *
 * <p>The window durations can be configured via {@link AutoScalarConfiguration}, allowing for
 * dynamic adjustment of scaling behavior. This class supports future extension to dynamically
 * compute window sizes, enabling acceleration or deceleration of auto-scaling as needed.
 *
 * <h2>Usage</h2>
 *
 * <pre>
 *   AutoScalarConfiguration config = ...;
 *   ScaleWindowManager windowManager = new ScaleWindowManager(config);
 *   Duration downScaleWindow = windowManager.getDownScaleWindowDuration();
 *   Duration upScaleWindow = windowManager.getUpScaleWindowDuration();
 *   Duration hibernateWindow = windowManager.getHibernateWindowDuration();
 * </pre>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is immutable and thread-safe.
 *
 * @see AutoScalarConfiguration
 */
public class ScaleWindowManager {
  private final Duration downScaleWindowDuration;
  private final Duration upScaleWindowDuration;
  private final Duration hibernateWindowDuration;

  /**
   * Constructs a new ScaleWindowManager with default configuration.
   *
   * <p>This constructor creates a ScaleWindowManager using the default AutoScalarConfiguration. The
   * window durations will be set to their default values as defined in the configuration.
   */
  public ScaleWindowManager() {
    this(new AutoScalarConfiguration());
  }

  /**
   * Constructs a new ScaleWindowManager with the specified configuration.
   *
   * <p>This constructor creates a ScaleWindowManager using the provided AutoScalarConfiguration.
   * The window durations will be extracted from the configuration and stored as immutable values.
   *
   * @param config the auto-scalar configuration containing window duration settings
   */
  public ScaleWindowManager(AutoScalarConfiguration config) {
    this.downScaleWindowDuration = config.getDownScaleWindowDuration();
    this.upScaleWindowDuration = config.getUpScaleWindowDuration();
    this.hibernateWindowDuration = config.getHibernateWindowDuration();
  }

  /**
   * Gets the down-scale window duration.
   *
   * <p>This duration represents the time window used for down-scaling decisions. The auto-scalar
   * will wait for this duration before making down-scaling decisions to ensure stability and
   * prevent rapid scaling oscillations.
   *
   * @return the down-scale window duration
   */
  public Duration getDownScaleWindowDuration() {
    return downScaleWindowDuration;
  }

  /**
   * Gets the up-scale window duration.
   *
   * <p>This duration represents the time window used for up-scaling decisions. The auto-scalar will
   * wait for this duration before making up-scaling decisions to ensure that the increased load is
   * sustained and not just a temporary spike.
   *
   * @return the up-scale window duration
   */
  public Duration getUpScaleWindowDuration() {
    return upScaleWindowDuration;
  }

  /**
   * Gets the hibernate window duration.
   *
   * <p>This duration represents the time window used for hibernation decisions. The auto-scalar
   * will wait for this duration before considering hibernation of resources to ensure that the low
   * load condition is sustained and not just a temporary dip.
   *
   * @return the hibernate window duration
   */
  public Duration getHibernateWindowDuration() {
    return hibernateWindowDuration;
  }
}
