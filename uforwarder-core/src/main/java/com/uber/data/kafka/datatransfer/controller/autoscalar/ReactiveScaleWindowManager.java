package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages reactive scaling windows that adapt based on system load conditions.
 *
 * <p>This class extends {@link ScaleWindowManager} to provide reactive scaling capabilities. It
 * dynamically adjusts scaling window durations based on real-time system load measurements,
 * enabling more responsive and adaptive auto-scaling behavior.
 *
 * <p>The manager uses a sliding window approach to collect load samples and computes
 * percentile-based load indicators. When the window becomes mature (has sufficient samples), it
 * triggers a reactive calculation to adjust the down-scale window duration.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Load-based window duration adjustment
 *   <li>Sliding window sample collection
 *   <li>Percentile-based load calculation
 *   <li>Thread-safe state management
 *   <li>Configurable load thresholds
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * AutoScalarConfiguration config = new AutoScalarConfiguration();
 * Ticker ticker = Ticker.systemTicker();
 * ReactiveScaleWindowCalculator calculator = new ReactiveScaleWindowCalculator();
 *
 * ReactiveScaleWindowManager manager = new ReactiveScaleWindowManager(config, ticker, calculator);
 *
 * // Process load samples
 * manager.onSample(0.8);  // 80% capacity utilization
 * manager.onSample(1.2);  // 120% capacity utilization (overloaded)
 *
 * // Get current window durations
 * Duration downScaleWindow = manager.getDownScaleWindowDuration();
 * }</pre>
 *
 * <p>This class is thread-safe and can be used in multi-threaded environments.
 *
 * @see ScaleWindowManager
 * @see ReactiveScaleWindowCalculator
 * @since 1.0
 */
public class ReactiveScaleWindowManager extends ScaleWindowManager {

  /**
   * Minimum load threshold for scaling calculations.
   *
   * <p>Load values below this threshold are considered to indicate underutilization.
   */
  private static final double MIN_LOAD = 0.5;

  /**
   * Maximum load threshold for scaling calculations.
   *
   * <p>Load values above this threshold are considered to indicate overutilization.
   */
  private static final double MAX_LOAD = 2.0;

  /**
   * Percentile used for load calculation (50th percentile).
   *
   * <p>This represents the median load value within the sliding window.
   */
  private static final double WINDOW_PERCENTILE = 0.5;

  private final Ticker ticker;
  private final ScaleWindow.Builder windowBuilder;
  private final ReactiveScaleWindowCalculator reactiveScaleWindowCalculator;
  private final ScaleStatusStore scaleStatusStore;
  private final AutoScalarConfiguration config;
  private volatile State state;
  private final Duration minDownScaleWindowDuration;
  private final Duration maxDownScaleWindowDuration;

  /**
   * Constructs a new ReactiveScaleWindowManager with the specified configuration and dependencies.
   *
   * <p>This constructor initializes the reactive scaling manager with the provided configuration,
   * ticker for time measurements, and calculator for window duration adjustments. It sets up the
   * initial state and window builder for load sample collection.
   *
   * @param autoScalingConfig the auto-scaling configuration containing window settings
   * @param ticker the ticker to use for time-based measurements
   * @param reactiveScaleWindowCalculator the calculator for reactive window adjustments
   */
  ReactiveScaleWindowManager(
      ScaleStatusStore scaleStatusStore,
      AutoScalarConfiguration autoScalingConfig,
      Ticker ticker,
      ReactiveScaleWindowCalculator reactiveScaleWindowCalculator) {
    super(autoScalingConfig);
    this.config = autoScalingConfig;
    this.scaleStatusStore = scaleStatusStore;
    this.ticker = ticker;
    this.reactiveScaleWindowCalculator = reactiveScaleWindowCalculator;
    this.windowBuilder =
        ScaleWindow.newBuilder()
            .withWindowDurationSupplier(
                () -> autoScalingConfig.getReactiveScaleWindowDuration().toNanos())
            .withTicker(ticker);
    this.state = new State(autoScalingConfig.getDownScaleWindowDuration());
    this.minDownScaleWindowDuration =
        Duration.ofSeconds(
            (long)
                (config.getDownScaleWindowDuration().toSeconds()
                    * config.getDownScaleWindowMinRatio()));
    this.maxDownScaleWindowDuration = config.getDownScaleWindowDuration();
  }

  /**
   * Processes a load sample and potentially triggers reactive window adjustments.
   *
   * <p>This method accepts a load measurement and adds it to the sliding window. The load value
   * represents the ratio between supply and demand of capacity:
   *
   * <ul>
   *   <li>load &lt; 1.0 indicates demand is smaller than supply (underutilization)
   *   <li>load = 1.0 indicates demand matches supply (balanced)
   *   <li>load &gt; 1.0 indicates demand is more than supply (overutilization)
   * </ul>
   *
   * <p>When the sliding window becomes mature (has collected sufficient samples), the method may
   * trigger a reactive calculation to adjust the down-scale window duration based on the current
   * load conditions.
   *
   * @param load the load measurement representing capacity utilization ratio
   */
  public synchronized void onSample(double load) {
    Optional<State> nextState = this.state.onSample(load);
    nextState.ifPresent(state -> this.state = state);
  }

  @Override
  public Duration getDownScaleWindowDuration() {
    return state.downScaleWindowDuration;
  }

  /**
   * Validates scale window - new value must be smaller than max value - new value must be larger
   * than min value - new value must be limited by change rate
   *
   * @param newValue
   * @param oldValue
   * @return
   */
  @VisibleForTesting
  protected Duration validateDownScaleWindow(Duration newValue, Duration oldValue) {
    newValue =
        Collections.max(
            List.of(
                minDownScaleWindowDuration,
                Duration.ofSeconds(
                    (long) (oldValue.getSeconds() * (1 - config.getReactiveScaleWindowRate()))),
                newValue));

    newValue =
        Collections.min(
            List.of(
                maxDownScaleWindowDuration,
                Duration.ofSeconds(
                    (long) (oldValue.getSeconds() * (1 + config.getReactiveScaleWindowRate()))),
                newValue));

    return newValue;
  }

  @VisibleForTesting
  protected synchronized void setDownScaleWindowDuration(Duration downScaleWindowDuration) {
    state = new State(downScaleWindowDuration);
  }

  /**
   * Represents the internal state of the reactive scaling window manager.
   *
   * <p>This inner class manages the current scaling window state, including the down-scale window
   * duration, sample collection window, and timing information. It handles the transition logic
   * when the window becomes mature and triggers reactive adjustments.
   *
   * <p>The state is thread-safe and uses atomic operations to ensure proper state transitions.
   */
  private class State {
    private final Duration downScaleWindowDuration;
    private final long stateTimeNano;
    private final ScaleWindow scaleWindow;
    private final AtomicReference<Boolean> closed = new AtomicReference<>(false);

    /**
     * Constructs a new State with the specified down-scale window duration.
     *
     * <p>This constructor initializes the state with the given down-scale window duration, records
     * the start time, and creates a new scale window for sample collection.
     *
     * @param downScaleWindowDuration the initial down-scale window duration
     */
    private State(Duration downScaleWindowDuration) {
      this.stateTimeNano = ticker.read();
      this.scaleWindow = windowBuilder.build(MIN_LOAD, MAX_LOAD);
      this.downScaleWindowDuration = downScaleWindowDuration;
    }

    /**
     * Processes a load sample and determines if a state transition is needed.
     *
     * <p>This method adds the load sample to the scale window and checks if the window has become
     * mature. When mature, it calculates the median load and triggers a reactive window adjustment
     * if the state hasn't already been closed.
     *
     * <p>The method uses atomic operations to ensure that only one state transition occurs per
     * mature window, preventing race conditions in multi-threaded environments.
     *
     * @param load the load measurement to process
     * @return an Optional containing the next state if a transition should occur, empty otherwise
     */
    Optional<State> onSample(double load) {
      scaleWindow.add(load);
      if (scaleWindow.isMature()) {
        if (closed.compareAndSet(false, true)) {
          double systemLoad = scaleWindow.getByPercentile(WINDOW_PERCENTILE);
          Duration newDownScaleWindowDuration =
              reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
                  scaleStatusStore.snapshot(), systemLoad, stateTimeNano, downScaleWindowDuration);

          newDownScaleWindowDuration =
              validateDownScaleWindow(newDownScaleWindowDuration, downScaleWindowDuration);
          return Optional.of(new State(newDownScaleWindowDuration));
        }
      }

      return Optional.empty();
    }
  }
}
