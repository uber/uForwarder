package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.JobGroupScaleStatusSnapshot;
import com.uber.data.kafka.datatransfer.ScaleComputerSnapshot;
import com.uber.data.kafka.datatransfer.ScaleStateSnapshot;
import com.uber.data.kafka.datatransfer.WindowSnapshot;
import com.uber.data.kafka.datatransfer.WindowedComputerSnapshot;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ScalePredictionUtils {

  /**
   * Predicts a scale prediction from a job group scale status snapshot.
   *
   * @param snapshot
   * @return the scale prediction
   */
  public static Optional<ScalePrediction> predict(JobGroupScaleStatusSnapshot snapshot) {
    return predict(snapshot.getScaleStateSnapshot());
  }

  /**
   * Predicts a scale prediction from a scale state snapshot.
   *
   * @param scaleStateSnapshot
   * @return the scale prediction
   */
  public static Optional<ScalePrediction> predict(ScaleStateSnapshot scaleStateSnapshot) {
    return scaleStateSnapshot.getScaleComputerSnapshotsList().stream()
        .map(s -> predict(s, scaleStateSnapshot.getScale()))
        .map(optional -> optional.orElse(null))
        .filter(Objects::nonNull)
        .sorted(Comparator.comparingLong(ScalePrediction::getCountdownNanos))
        .findFirst();
  }

  /**
   * Predicts a scale prediction from a scale computer snapshot.
   *
   * @param snapshot
   * @param currentScale
   * @return the scale prediction
   */
  public static Optional<ScalePrediction> predict(
      ScaleComputerSnapshot snapshot, double currentScale) {
    if (snapshot.hasWindowedComputerSnapshot()) {
      return predict(snapshot.getWindowedComputerSnapshot(), currentScale);
    }
    return Optional.empty();
  }

  /**
   * Predicts a scale prediction from a windowed computer snapshot. The prediction is valid only if
   * the number of samples in the window is greater than the minimum size in samples. The prediction
   * is valid only if the proposal is within the lower and upper boundaries.
   *
   * @param snapshot
   * @param currentScale
   * @return the scale prediction
   */
  public static Optional<ScalePrediction> predict(
      WindowedComputerSnapshot snapshot, double currentScale) {
    WindowSnapshot windowSnapshot = snapshot.getWindowSnapshot();
    if (windowSnapshot.getSizeInSamples() < windowSnapshot.getMinSizeInSamples()) {
      return Optional.empty();
    }
    double proposal = snapshot.getPercentileScale();
    if (proposal >= snapshot.getLowerBoundary() && proposal <= snapshot.getUpperBoundary()) {
      long nanosToMatures =
          TimeUnit.SECONDS.toNanos(
              windowSnapshot.getMinSizeInSeconds() - windowSnapshot.getSizeInSeconds());
      return Optional.of(new ScalePrediction(nanosToMatures, currentScale, proposal));
    }

    return Optional.empty();
  }

  /**
   * Checks if a scale prediction indicates a downscale.
   *
   * @param prediction The scale prediction to check.
   * @return True if the prediction indicates a downscale, false otherwise.
   */
  public static boolean isDownScale(ScalePrediction prediction) {
    return prediction.getFutureScale() > 0 && prediction.getDiff() < 0;
  }

  /**
   * Finds index of first element in a descending sorted array that is strictly smaller than the
   * target value.
   *
   * @param arr The descending sorted array to search within.
   * @param target The value for which to find the next smaller element.
   * @return The index of first element strictly smaller than the target, or -1 if no such element
   *     exists.
   */
  public static int findFirstSmaller(double[] arr, double target) {
    if (arr == null || arr.length == 0) {
      return -1;
    }

    int low = 0;
    int high = arr.length - 1;
    while (low < high - 1) {
      int mid = low + (high - low) / 2;
      if (arr[mid] < target) {
        high = mid;
      } else {
        low = mid + 1;
      }
    }
    if (arr[low] < target) {
      return low;
    } else if (arr[high] < target) {
      return high;
    } else {
      return -1;
    }
  }
}
