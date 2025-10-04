package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.uber.data.kafka.datatransfer.ScaleStoreSnapshot;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Calculates reactive scaling window durations based on system load conditions.
 *
 * <p>This class provides adaptive window size calculation for auto-scaling operations. It
 * dynamically adjusts scaling window durations based on current system load indicators to optimize
 * scaling responsiveness and system stability.
 *
 * <p>The calculator implements reactive scaling logic where:
 *
 * <ul>
 *   <li>When system load indicates tight capacity, window sizes are reduced for faster scaling
 *   <li>When system load is comfortable, window sizes are increased or maintained for stability
 *   <li>Changes are based on load thresholds and timing considerations
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * ReactiveScaleWindowCalculator calculator = new ReactiveScaleWindowCalculator();
 *
 * double currentLoad = 0.85; // 85% system load
 * long lastModifyTime = System.nanoTime();
 * Duration currentWindow = Duration.ofMinutes(5);
 *
 * Duration newWindow = calculator.calculateDownScaleWindowDuration(
 *     currentLoad, lastModifyTime, currentWindow);
 * }</pre>
 *
 * <p>This class is stateless and thread-safe.
 *
 * @since 1.0
 */
@ThreadSafe
public class ReactiveScaleWindowCalculator {
  private Ticker ticker;

  public ReactiveScaleWindowCalculator() {
    this(Ticker.systemTicker());
  }

  @VisibleForTesting
  protected ReactiveScaleWindowCalculator(Ticker ticker) {
    this.ticker = ticker;
  }

  /**
   * Reactively computes the down-scale window duration based on system load conditions.
   *
   * <p>This method implements adaptive scaling logic for down-scaling operations. When the system
   * load indicator shows that capacity is tight (high load), the method reduces the down-scale
   * window size to enable faster workload reduction. Conversely, when system load is comfortable,
   * it increases or maintains the window size for stability.
   *
   * <p>The calculation takes into account:
   *
   * <ul>
   *   <li>Current system load indicator indicates ratio between capacity demand and supply
   *   <li>Last modification time to prevent excessive changes
   *   <li>Current window duration as the baseline for adjustments
   * </ul>
   *
   * <p>Load thresholds and timing considerations are used to determine appropriate window size
   * adjustments. The goal is to balance responsiveness with stability in the auto-scaling system.
   *
   * @param load relative load to threshold
   * @param stateTimeNano last modified time of down scale window duration
   * @param currentDownScaleWindowDuration last value of down scale window size
   * @return the calculated new down scale window duration
   */
  Duration calculateDownScaleWindowDuration(
      ScaleStoreSnapshot scaleStoreSnapshot,
      double load,
      long stateTimeNano,
      Duration currentDownScaleWindowDuration) {

    if (load < 1.0) {
      // addictive increase
      long elapsedNanos = Math.max(0, ticker.read() - stateTimeNano);
      return currentDownScaleWindowDuration.plus(Duration.ofNanos(elapsedNanos));
    }

    // multiply decrease
    double totalScale =
        scaleStoreSnapshot.getJobGroupSnapshotList().stream()
            .mapToDouble(o -> o.getScaleStateSnapshot().getScale())
            .sum();

    double expectedScale = totalScale / load;
    double targetScaleDiff = expectedScale - totalScale;

    List<ScalePrediction> downScalePredications =
        scaleStoreSnapshot.getJobGroupSnapshotList().stream()
            .map(ScalePredictionUtils::predict)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .filter(ScalePredictionUtils::isDownScale)
            .sorted(Comparator.comparingLong(ScalePrediction::getCountdownNanos))
            .collect(Collectors.toList());

    if (downScalePredications.isEmpty()) {
      // no candidate can downscale, keep downscale window as is
      return currentDownScaleWindowDuration;
    }

    // build index
    double[] accumulatedScaleDiffs = new double[downScalePredications.size()];
    double sumDiff = 0;
    int i = 0;
    for (ScalePrediction prediction : downScalePredications) {
      sumDiff += prediction.getDiff();
      accumulatedScaleDiffs[i++] = sumDiff;
    }

    // find targetScaleDiff on index
    int index = ScalePredictionUtils.findFirstSmaller(accumulatedScaleDiffs, targetScaleDiff);
    if (index == -1) {
      // can't find such element, take best effort
      index = accumulatedScaleDiffs.length - 1;
    }

    // compute proposed down scale window size
    Duration duration = Duration.ofNanos(downScalePredications.get(index).getCountdownNanos());
    return currentDownScaleWindowDuration.minus(duration);
  }
}
