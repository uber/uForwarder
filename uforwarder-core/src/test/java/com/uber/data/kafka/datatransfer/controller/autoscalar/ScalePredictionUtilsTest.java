package com.uber.data.kafka.datatransfer.controller.autoscalar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.uber.data.kafka.datatransfer.JobGroupScaleStatusSnapshot;
import com.uber.data.kafka.datatransfer.ScaleComputerSnapshot;
import com.uber.data.kafka.datatransfer.ScaleStateSnapshot;
import com.uber.data.kafka.datatransfer.WindowSnapshot;
import com.uber.data.kafka.datatransfer.WindowedComputerSnapshot;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Optional;
import org.junit.Test;

public class ScalePredictionUtilsTest extends FievelTestBase {

  @Test
  public void testFindFirstSmallerWithNormalCase() {
    // Test with a normal descending sorted array
    double[] arr = {10.0, 8.0, 6.0, 4.0, 2.0};
    double target = 7.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 2 (value 6.0) as first smaller than 7.0", 2, result);
  }

  @Test
  public void testFindFirstSmallerWithTargetAtBeginning() {
    // Test when target is larger than all elements
    double[] arr = {10.0, 8.0, 6.0, 4.0, 2.0};
    double target = 12.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 0 (value 10.0) as first smaller than 12.0", 0, result);
  }

  @Test
  public void testFindFirstSmallerWithTargetAtEnd() {
    // Test when target is smaller than all elements
    double[] arr = {10.0, 8.0, 6.0, 4.0, 2.0};
    double target = 1.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should return -1 when no element is smaller than target", -1, result);
  }

  @Test
  public void testFindFirstSmallerWithTargetEqualToElements() {
    // Test when target equals some elements
    double[] arr = {10.0, 8.0, 6.0, 4.0, 2.0};
    double target = 6.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 3 (value 4.0) as first smaller than 6.0", 3, result);
  }

  @Test
  public void testFindFirstSmallerWithSingleElementArray() {
    // Test with single element array
    double[] arr = {5.0};
    double target = 6.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 0 (value 5.0) as smaller than 6.0", 0, result);
  }

  @Test
  public void testFindFirstSmallerWithSingleElementArrayNoMatch() {
    // Test with single element array where no match
    double[] arr = {5.0};
    double target = 4.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should return -1 when single element is not smaller than target", -1, result);
  }

  @Test
  public void testFindFirstSmallerWithNegativeValues() {
    // Test with negative values
    double[] arr = {5.0, 3.0, 1.0, -1.0, -3.0, -5.0};
    double target = 0.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 3 (value -1.0) as first smaller than 0.0", 3, result);
  }

  @Test
  public void testFindFirstSmallerWithDecimalValues() {
    // Test with decimal values
    double[] arr = {10.5, 8.3, 6.1, 4.9, 2.7, 0.5};
    double target = 7.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 2 (value 6.1) as first smaller than 7.0", 2, result);
  }

  @Test
  public void testFindFirstSmallerWithTargetBetweenElements() {
    // Test when target falls between two elements
    double[] arr = {10.0, 8.0, 6.0, 4.0, 2.0};
    double target = 5.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 3 (value 4.0) as first smaller than 5.0", 3, result);
  }

  @Test
  public void testFindFirstSmallerWithTargetEqualToFirstElement() {
    // Test when target equals the first element
    double[] arr = {10.0, 8.0, 6.0, 4.0, 2.0};
    double target = 10.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 1 (value 8.0) as first smaller than 10.0", 1, result);
  }

  @Test
  public void testFindFirstSmallerWithTargetEqualToLastElement() {
    // Test when target equals the last element
    double[] arr = {10.0, 8.0, 6.0, 4.0, 2.0};
    double target = 2.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should return -1 when target equals last element", -1, result);
  }

  @Test
  public void testFindFirstSmallerWithVerySmallTarget() {
    // Test with very small target value
    double[] arr = {10.0, 8.0, 6.0, 4.0, 2.0};
    double target = -1000.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should return -1 when target is much smaller than all elements", -1, result);
  }

  @Test
  public void testFindFirstSmallerWithVeryLargeTarget() {
    // Test with very large target value
    double[] arr = {10.0, 8.0, 6.0, 4.0, 2.0};
    double target = 1000.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 0 (value 10.0) when target is much larger", 0, result);
  }

  @Test
  public void testFindFirstSmallerWithInfinity() {
    // Test with infinity values
    double[] arr = {Double.POSITIVE_INFINITY, 10.0, 8.0, 6.0, 4.0, 2.0};
    double target = 5.0;

    int result = ScalePredictionUtils.findFirstSmaller(arr, target);

    assertEquals("Should find index 4 (value 4.0) as first smaller than 5.0", 4, result);
  }

  @Test
  public void testFindFirstSmallerWithEmptyInput() {
    double[] arr = {};
    double target = 5.0;
    int result = ScalePredictionUtils.findFirstSmaller(arr, target);
    assertEquals(-1, result);
  }

  // Tests for isDownScale function

  @Test
  public void testIsDownScaleWithValidDownScale() {
    // Test with valid down-scale scenario: current=10.0, future=5.0 (scaling down)
    ScalePrediction prediction = new ScalePrediction(1000L, 10.0, 5.0);

    boolean result = ScalePredictionUtils.isDownScale(prediction);

    assertTrue("Should return true for valid down-scale (future=5.0 > 0, diff=-5.0 < 0)", result);
  }

  @Test
  public void testIsDownScaleWithUpScale() {
    // Test with up-scale scenario: current=5.0, future=10.0 (scaling up)
    ScalePrediction prediction = new ScalePrediction(1000L, 5.0, 10.0);

    boolean result = ScalePredictionUtils.isDownScale(prediction);

    assertFalse("Should return false for up-scale (future=10.0 > 0, diff=5.0 > 0)", result);
  }

  @Test
  public void testIsDownScaleWithNoChange() {
    // Test with no scale change: current=5.0, future=5.0
    ScalePrediction prediction = new ScalePrediction(1000L, 5.0, 5.0);

    boolean result = ScalePredictionUtils.isDownScale(prediction);

    assertFalse("Should return false for no change (future=5.0 > 0, diff=0.0 >= 0)", result);
  }

  @Test
  public void testIsDownScaleWithZeroFutureScale() {
    // Test with zero future scale: current=5.0, future=0.0
    ScalePrediction prediction = new ScalePrediction(1000L, 5.0, 0.0);

    boolean result = ScalePredictionUtils.isDownScale(prediction);

    assertFalse("Should return false when future scale is zero (future=0.0 <= 0)", result);
  }

  @Test
  public void testIsDownScaleWithNegativeFutureScale() {
    // Test with negative future scale: current=5.0, future=-1.0
    ScalePrediction prediction = new ScalePrediction(1000L, 5.0, -1.0);

    boolean result = ScalePredictionUtils.isDownScale(prediction);

    assertFalse("Should return false when future scale is negative (future=-1.0 <= 0)", result);
  }

  // Tests for predict function with WindowedComputerSnapshot

  @Test
  public void testPredictWithValidWindowedComputerSnapshot() {
    // Test with valid prediction: sufficient samples and proposal within boundaries
    WindowedComputerSnapshot snapshot =
        createWindowedComputerSnapshot(
            2.0, // lowerBoundary
            8.0, // upperBoundary
            3.5, // percentileScale (within boundaries)
            10, // sizeInSamples
            5, // minSizeInSamples
            100L, // sizeInSeconds
            300L // minSizeInSeconds
            );
    double currentScale = 5.0;

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot, currentScale);

    assertTrue("Should return a valid prediction", result.isPresent());
    ScalePrediction prediction = result.get();
    assertEquals(
        "Countdown should be 200 seconds in nanos",
        200_000_000_000L,
        prediction.getCountdownNanos());
    assertEquals("Current scale should match", currentScale, prediction.getCurrentScale(), 0.001);
    assertEquals(
        "Future scale should match percentile scale", 3.5, prediction.getFutureScale(), 0.001);
  }

  @Test
  public void testPredictWithInsufficientSamples() {
    // Test with insufficient samples: sizeInSamples < minSizeInSamples
    WindowedComputerSnapshot snapshot =
        createWindowedComputerSnapshot(
            2.0, // lowerBoundary
            8.0, // upperBoundary
            3.5, // percentileScale
            3, // sizeInSamples (less than minSizeInSamples)
            5, // minSizeInSamples
            100L, // sizeInSeconds
            300L // minSizeInSeconds
            );
    double currentScale = 5.0;

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot, currentScale);

    assertFalse("Should return empty when insufficient samples", result.isPresent());
  }

  @Test
  public void testPredictWithExactMinimumSamples() {
    // Test with exact minimum samples: sizeInSamples == minSizeInSamples
    WindowedComputerSnapshot snapshot =
        createWindowedComputerSnapshot(
            2.0, // lowerBoundary
            8.0, // upperBoundary
            3.5, // percentileScale
            5, // sizeInSamples (equals minSizeInSamples)
            5, // minSizeInSamples
            100L, // sizeInSeconds
            300L // minSizeInSeconds
            );
    double currentScale = 5.0;

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot, currentScale);

    assertTrue("Should return a valid prediction when samples equal minimum", result.isPresent());
  }

  @Test
  public void testPredictWithProposalBelowLowerBoundary() {
    // Test with proposal below lower boundary
    WindowedComputerSnapshot snapshot =
        createWindowedComputerSnapshot(
            2.0, // lowerBoundary
            8.0, // upperBoundary
            1.5, // percentileScale (below lowerBoundary)
            10, // sizeInSamples
            5, // minSizeInSamples
            100L, // sizeInSeconds
            300L // minSizeInSeconds
            );
    double currentScale = 5.0;

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot, currentScale);

    assertFalse("Should return empty when proposal below lower boundary", result.isPresent());
  }

  @Test
  public void testPredictWithProposalAboveUpperBoundary() {
    // Test with proposal above upper boundary
    WindowedComputerSnapshot snapshot =
        createWindowedComputerSnapshot(
            2.0, // lowerBoundary
            8.0, // upperBoundary
            9.0, // percentileScale (above upperBoundary)
            10, // sizeInSamples
            5, // minSizeInSamples
            100L, // sizeInSeconds
            300L // minSizeInSeconds
            );
    double currentScale = 5.0;

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot, currentScale);

    assertFalse("Should return empty when proposal above upper boundary", result.isPresent());
  }

  // Tests for predict function with ScaleComputerSnapshot

  @Test
  public void testPredictWithValidScaleComputerSnapshot() {
    // Test with valid ScaleComputerSnapshot that has WindowedComputerSnapshot
    ScaleComputerSnapshot snapshot =
        ScaleComputerSnapshot.newBuilder()
            .setWindowedComputerSnapshot(
                createWindowedComputerSnapshot(
                    2.0, // lowerBoundary
                    8.0, // upperBoundary
                    3.5, // percentileScale
                    10, // sizeInSamples
                    5, // minSizeInSamples
                    100L, // sizeInSeconds
                    300L // minSizeInSeconds
                    ))
            .build();
    double currentScale = 5.0;

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot, currentScale);

    assertTrue("Should return a valid prediction", result.isPresent());
    assertEquals(
        "Future scale should match percentile scale", 3.5, result.get().getFutureScale(), 0.001);
  }

  @Test
  public void testPredictWithEmptyScaleComputerSnapshot() {
    // Test with ScaleComputerSnapshot that has no WindowedComputerSnapshot
    ScaleComputerSnapshot snapshot = ScaleComputerSnapshot.newBuilder().build();
    double currentScale = 5.0;

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot, currentScale);

    assertFalse("Should return empty when no WindowedComputerSnapshot", result.isPresent());
  }

  @Test
  public void testPredictWithDefaultScaleComputerSnapshot() {
    // Test with default ScaleComputerSnapshot
    ScaleComputerSnapshot snapshot = ScaleComputerSnapshot.getDefaultInstance();
    double currentScale = 5.0;

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot, currentScale);

    assertFalse("Should return empty for default instance", result.isPresent());
  }

  // Tests for predict function with ScaleStateSnapshot

  @Test
  public void testPredictWithValidScaleStateSnapshot() {
    // Test with valid ScaleStateSnapshot containing valid computer snapshots
    ScaleStateSnapshot snapshot =
        ScaleStateSnapshot.newBuilder()
            .addScaleComputerSnapshots(
                ScaleComputerSnapshot.newBuilder()
                    .setWindowedComputerSnapshot(
                        createWindowedComputerSnapshot(
                            2.0, // lowerBoundary
                            8.0, // upperBoundary
                            3.5, // percentileScale
                            10, // sizeInSamples
                            5, // minSizeInSamples
                            100L, // sizeInSeconds
                            300L // minSizeInSeconds
                            ))
                    .build())
            .setScale(5.0)
            .build();

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot);

    assertTrue("Should return a valid prediction", result.isPresent());
    assertEquals("Current scale should match", 5.0, result.get().getCurrentScale(), 0.001);
  }

  @Test
  public void testPredictWithEmptyScaleStateSnapshot() {
    // Test with ScaleStateSnapshot containing no computer snapshots
    ScaleStateSnapshot snapshot = ScaleStateSnapshot.newBuilder().setScale(5.0).build();

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot);

    assertFalse("Should return empty when no computer snapshots", result.isPresent());
  }

  @Test
  public void testPredictWithInvalidComputerSnapshots() {
    // Test with ScaleStateSnapshot containing invalid computer snapshots
    ScaleStateSnapshot snapshot =
        ScaleStateSnapshot.newBuilder()
            .addScaleComputerSnapshots(ScaleComputerSnapshot.newBuilder().build()) // Empty snapshot
            .addScaleComputerSnapshots(
                ScaleComputerSnapshot.newBuilder()
                    .setWindowedComputerSnapshot(
                        createWindowedComputerSnapshot(
                            2.0, // lowerBoundary
                            8.0, // upperBoundary
                            1.5, // percentileScale (below lowerBoundary - invalid)
                            10, // sizeInSamples
                            5, // minSizeInSamples
                            100L, // sizeInSeconds
                            300L // minSizeInSeconds
                            ))
                    .build())
            .setScale(5.0)
            .build();

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot);

    assertFalse("Should return empty when all computer snapshots are invalid", result.isPresent());
  }

  @Test
  public void testPredictWithMultipleValidComputerSnapshots() {
    // Test with ScaleStateSnapshot containing multiple valid computer snapshots
    // Should return the one with the smallest countdown (sorted by countdown)
    ScaleStateSnapshot snapshot =
        ScaleStateSnapshot.newBuilder()
            .addScaleComputerSnapshots(
                ScaleComputerSnapshot.newBuilder()
                    .setWindowedComputerSnapshot(
                        createWindowedComputerSnapshot(
                            2.0, // lowerBoundary
                            8.0, // upperBoundary
                            3.5, // percentileScale
                            10, // sizeInSamples
                            5, // minSizeInSamples
                            100L, // sizeInSeconds
                            300L // minSizeInSeconds (countdown = 200s)
                            ))
                    .build())
            .addScaleComputerSnapshots(
                ScaleComputerSnapshot.newBuilder()
                    .setWindowedComputerSnapshot(
                        createWindowedComputerSnapshot(
                            1.0, // lowerBoundary
                            5.0, // upperBoundary
                            2.5, // percentileScale
                            10, // sizeInSamples
                            5, // minSizeInSamples
                            100L, // sizeInSeconds
                            150L // minSizeInSeconds (countdown = 50s - smaller)
                            ))
                    .build())
            .setScale(5.0)
            .build();

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot);

    assertTrue("Should return a valid prediction", result.isPresent());
    assertEquals(
        "Should return prediction with smallest countdown",
        50_000_000_000L,
        result.get().getCountdownNanos());
  }

  // Tests for predict function with JobGroupScaleStatusSnapshot

  @Test
  public void testPredictWithValidJobGroupScaleStatusSnapshot() {
    // Test with valid JobGroupScaleStatusSnapshot
    JobGroupScaleStatusSnapshot snapshot =
        JobGroupScaleStatusSnapshot.newBuilder()
            .setCluster("test-cluster")
            .setTopic("test-topic")
            .setConsumerGroup("test-consumer-group")
            .setScaleStateSnapshot(
                ScaleStateSnapshot.newBuilder()
                    .addScaleComputerSnapshots(
                        ScaleComputerSnapshot.newBuilder()
                            .setWindowedComputerSnapshot(
                                createWindowedComputerSnapshot(
                                    2.0, // lowerBoundary
                                    8.0, // upperBoundary
                                    3.5, // percentileScale
                                    10, // sizeInSamples
                                    5, // minSizeInSamples
                                    100L, // sizeInSeconds
                                    300L // minSizeInSeconds
                                    ))
                            .build())
                    .setScale(5.0)
                    .build())
            .build();

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot);

    assertTrue("Should return a valid prediction", result.isPresent());
    assertEquals("Current scale should match", 5.0, result.get().getCurrentScale(), 0.001);
  }

  @Test
  public void testPredictWithEmptyJobGroupScaleStatusSnapshot() {
    // Test with JobGroupScaleStatusSnapshot containing empty scale state
    JobGroupScaleStatusSnapshot snapshot =
        JobGroupScaleStatusSnapshot.newBuilder()
            .setCluster("test-cluster")
            .setTopic("test-topic")
            .setConsumerGroup("test-consumer-group")
            .setScaleStateSnapshot(ScaleStateSnapshot.newBuilder().setScale(5.0).build())
            .build();

    Optional<ScalePrediction> result = ScalePredictionUtils.predict(snapshot);

    assertFalse(
        "Should return empty when scale state has no computer snapshots", result.isPresent());
  }

  // Helper method to create WindowedComputerSnapshot for testing
  private WindowedComputerSnapshot createWindowedComputerSnapshot(
      double lowerBoundary,
      double upperBoundary,
      double percentileScale,
      int sizeInSamples,
      int minSizeInSamples,
      long sizeInSeconds,
      long minSizeInSeconds) {

    WindowSnapshot windowSnapshot =
        WindowSnapshot.newBuilder()
            .setSizeInSamples(sizeInSamples)
            .setMinSizeInSamples(minSizeInSamples)
            .setSizeInSeconds(sizeInSeconds)
            .setMinSizeInSeconds(minSizeInSeconds)
            .build();

    return WindowedComputerSnapshot.newBuilder()
        .setLowerBoundary(lowerBoundary)
        .setUpperBoundary(upperBoundary)
        .setPercentileScale(percentileScale)
        .setWindowSnapshot(windowSnapshot)
        .build();
  }
}
