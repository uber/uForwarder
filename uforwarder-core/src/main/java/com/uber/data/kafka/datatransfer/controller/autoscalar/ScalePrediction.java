package com.uber.data.kafka.datatransfer.controller.autoscalar;

/**
 * Represents a prediction for auto-scaling operations in data transfer pipelines.
 *
 * <p>This class encapsulates information about a predicted scale change, including the current
 * scale, the proposed future scale, and the time remaining until the prediction matures. It is used
 * by the auto-scalar system to make decisions about scaling up or down based on workload patterns
 * and performance metrics.
 *
 * <p>The prediction includes:
 *
 * <ul>
 *   <li><strong>Countdown</strong>: Time remaining in nanoseconds until the prediction matures
 *   <li><strong>Current Scale</strong>: The current scaling factor of the system
 *   <li><strong>Future Scale</strong>: The proposed scaling factor after the prediction matures
 * </ul>
 *
 * <p>This class implements {@link Comparable} to allow sorting predictions by their countdown time,
 * enabling the auto-scalar to prioritize predictions that will mature soonest.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * // Create a prediction for scaling down from 10.0 to 5.0 in 30 seconds
 * ScalePrediction prediction = new ScalePrediction(
 *     TimeUnit.SECONDS.toNanos(30),  // countdown
 *     10.0,                          // current scale
 *     5.0                            // future scale
 * );
 *
 * // Check if this is a down-scaling prediction
 * boolean isDownScale = ScalePredictionUtils.isDownScale(prediction);
 *
 * // Get the scale difference
 * double scaleDiff = prediction.getDiff(); // returns -5.0
 * }</pre>
 *
 * <p>This class is immutable and thread-safe, making it suitable for use in concurrent
 * environments.
 *
 * @see ScalePredictionUtils#isDownScale(ScalePrediction)
 * @since 1.0
 */
public class ScalePrediction {

  /**
   * The countdown time in nanoseconds until this prediction matures.
   *
   * <p>This value represents the time remaining before the prediction becomes actionable. A
   * positive value indicates the prediction is still maturing, while a zero or negative value
   * indicates the prediction has matured and can be acted upon immediately.
   */
  private final long countDownNanos;

  /**
   * The current scaling factor of the system.
   *
   * <p>This represents the current scale at which the system is operating. The scale factor
   * typically indicates the number of workers, replicas, or processing units allocated to handle
   * the workload.
   */
  private final double currentScale;

  /**
   * The proposed future scaling factor.
   *
   * <p>This represents the scale factor that the auto-scalar predicts should be applied once the
   * prediction matures. This value is determined by analyzing workload patterns, performance
   * metrics, and scaling policies.
   */
  private final double futureScale;

  /**
   * Constructs a new ScalePrediction with the specified parameters.
   *
   * <p>This constructor creates an immutable prediction object that encapsulates the scaling
   * information and countdown time. The prediction can be used by the auto-scalar system to make
   * scaling decisions.
   *
   * @param countDownNanos the countdown time in nanoseconds until the prediction matures. A
   *     positive value indicates the prediction is still maturing, while zero or negative indicates
   *     it has matured
   * @param currentScale the current scaling factor of the system. This should be a non-negative
   *     value representing the current allocation of resources
   * @param futureScale the proposed future scaling factor. This should be a non-negative value
   *     representing the recommended resource allocation
   */
  public ScalePrediction(long countDownNanos, double currentScale, double futureScale) {
    this.countDownNanos = countDownNanos;
    this.currentScale = currentScale;
    this.futureScale = futureScale;
  }

  /**
   * Gets the countdown time in nanoseconds until this prediction matures.
   *
   * <p>This method returns the time remaining before the prediction becomes actionable. The
   * interpretation of this value depends on the context:
   *
   * <ul>
   *   <li><strong>Positive values</strong>: The prediction is still maturing and should not be
   *       acted upon yet
   *   <li><strong>Zero</strong>: The prediction has just matured and can be acted upon
   *   <li><strong>Negative values</strong>: The prediction has matured and should have been acted
   *       upon already
   * </ul>
   *
   * @return the countdown time in nanoseconds until this prediction matures
   */
  public long getCountdownNanos() {
    return this.countDownNanos;
  }

  /**
   * Gets the proposed future scaling factor.
   *
   * <p>This method returns the scale factor that the auto-scalar predicts should be applied once
   * the prediction matures. This value is typically determined by analyzing workload patterns,
   * performance metrics, and scaling policies.
   *
   * <p>The future scale can be:
   *
   * <ul>
   *   <li><strong>Greater than current scale</strong>: Indicates a scale-up operation
   *   <li><strong>Less than current scale</strong>: Indicates a scale-down operation
   *   <li><strong>Equal to current scale</strong>: Indicates no change is needed
   * </ul>
   *
   * @return the proposed future scaling factor
   */
  public double getFutureScale() {
    return this.futureScale;
  }

  /**
   * Gets the current scaling factor of the system.
   *
   * <p>This method returns the scale factor at which the system is currently operating. The scale
   * factor typically represents the number of workers, replicas, or processing units allocated to
   * handle the workload.
   *
   * @return the current scaling factor of the system
   */
  public double getCurrentScale() {
    return this.currentScale;
  }

  /**
   * Calculates the difference between the future scale and current scale.
   *
   * <p>This method provides a convenient way to determine the magnitude and direction of the
   * proposed scale change. The returned value indicates:
   *
   * <ul>
   *   <li><strong>Positive values</strong>: The system should scale up (increase resources)
   *   <li><strong>Negative values</strong>: The system should scale down (decrease resources)
   *   <li><strong>Zero</strong>: No scale change is needed
   * </ul>
   *
   * <p>The absolute value of the difference indicates the magnitude of the scale change. For
   * example, a value of -2.0 means the system should scale down by 2 units, while a value of 1.5
   * means the system should scale up by 1.5 units.
   *
   * @return the difference between future scale and current scale (futureScale - currentScale)
   */
  public double getDiff() {
    return this.futureScale - this.currentScale;
  }
}
