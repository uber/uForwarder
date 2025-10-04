package com.uber.data.kafka.datatransfer.controller.autoscalar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.uber.data.kafka.datatransfer.JobGroupScaleStatusSnapshot;
import com.uber.data.kafka.datatransfer.ScaleComputerSnapshot;
import com.uber.data.kafka.datatransfer.ScaleStateSnapshot;
import com.uber.data.kafka.datatransfer.ScaleStoreSnapshot;
import com.uber.data.kafka.datatransfer.WindowSnapshot;
import com.uber.data.kafka.datatransfer.WindowedComputerSnapshot;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReactiveScaleWindowCalculatorTest {
  private ReactiveScaleWindowCalculator reactiveScaleWindowCalculator;
  private TestUtils.TestTicker ticker;

  @SuppressWarnings("ForbidClassloaderGetResourceInTests")
  @BeforeEach
  public void setUp() throws IOException {
    ticker = new TestUtils.TestTicker();
    reactiveScaleWindowCalculator = new ReactiveScaleWindowCalculator(ticker);
  }

  @Test
  public void testCalculateDownScaleWindowDuration() {
    // Test with normal load conditions
    double load = 0.8; // 80% capacity utilization
    long lastModifyNano = ticker.read();
    Duration currentValue = Duration.ofMinutes(5);

    Duration result =
        reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
            ScaleStoreSnapshot.getDefaultInstance(), load, lastModifyNano, currentValue);

    assertNotNull(result, "Result should not be null");
    assertEquals(currentValue, result, "Should return current value for normal load");
  }

  @Test
  public void testCalculateDownScaleWindowDurationWithTimeElapsed() {
    // Test with normal load conditions
    double load = 0.8; // 80% capacity utilization
    long lastModifyNano = ticker.read();
    Duration currentValue = Duration.ofMinutes(5);
    ticker.add(Duration.ofMinutes(1));
    Duration result =
        reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
            ScaleStoreSnapshot.getDefaultInstance(), load, lastModifyNano, currentValue);

    assertNotNull(result, "Result should not be null");
    assertEquals(
        currentValue.plus(Duration.ofMinutes(1)),
        result,
        "Should return current value for normal load");
  }

  @Test
  public void testCalculateDownScaleWindowDurationWithLargeLoad() {
    ScaleStoreSnapshot scaleStoreSnapshot =
        ScaleStoreSnapshot.newBuilder()
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    3.11,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(3.527, 4.212, 2.358, 104L, 300L, 20, 10),
                      new ScaleComputerParams(2.053, 2.6848, 2.095, 104, 3600, 20, 10),
                      new ScaleComputerParams(0.0, 0.0, 2.358, 104, 43200, 20, 10)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    1.64,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(1.976, 3.29, 1.663, 104, 300, 20, 10),
                      new ScaleComputerParams(0.823, 1.318, 1.639, 104, 3600, 20, 10),
                      new ScaleComputerParams(0.0, 0.0, 2.358, 104, 43200, 20, 10)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    0.0,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(0.000001, 1, 0.0, 104, 300, 20, 10),
                    }))
            .build();

    // Test with normal load conditions
    double load = 1.1; // 80% capacity utilization
    long lastModifyNano = ticker.read();
    Duration currentValue = Duration.ofDays(1);
    Duration result =
        reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
            scaleStoreSnapshot, load, lastModifyNano, currentValue);

    assertNotNull(result, "Result should not be null");
    Duration expected = Duration.ofSeconds((23 * 60 + 1) * 60 + 44);
    assertEquals(expected, result, "Should return reduced value for heavy load");
  }

  @Test
  public void testReduceDownScaleWindowDurationWithAllMatureWindow() {
    ScaleStoreSnapshot scaleStoreSnapshot =
        ScaleStoreSnapshot.newBuilder()
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    3.11,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(2.053, 2.6848, 2.395, 3300, 3600, 20, 10)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    1.64,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(1.176, 1.52, 1.28, 3000, 3600, 20, 10)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    3.213,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(2.31, 2.87, 2.78, 2700, 3600, 20, 10),
                    }))
            .build();

    // Test with normal load conditions
    double load = 1.1; // 80% capacity utilization
    long lastModifyNano = ticker.read();
    Duration currentValue = Duration.ofDays(1);
    Duration result =
        reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
            scaleStoreSnapshot, load, lastModifyNano, currentValue);

    assertNotNull(result, "Result should not be null");
    Duration expected = currentValue.minus(Duration.ofMinutes(10));
    assertEquals(expected, result, "Should return reduced value for heavy load");
  }

  @Test
  public void testReduceDownScaleWindowDurationWithNoMatureWindow() {
    ScaleStoreSnapshot scaleStoreSnapshot =
        ScaleStoreSnapshot.newBuilder()
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    3.11,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(2.053, 2.6848, 2.395, 3300, 3600, 20, 30)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    1.64,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(1.176, 1.52, 1.28, 3000, 3600, 20, 30)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    3.213,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(2.31, 2.87, 2.78, 2700, 3600, 20, 30),
                    }))
            .build();

    // Test with normal load conditions
    double load = 1.1; // 80% capacity utilization
    long lastModifyNano = ticker.read();
    Duration currentValue = Duration.ofDays(1);
    Duration result =
        reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
            scaleStoreSnapshot, load, lastModifyNano, currentValue);

    assertNotNull(result, "Result should not be null");
    assertEquals(currentValue, result, "Should return reduced value for heavy load");
  }

  @Test
  public void testReduceDownScaleWindowDurationScaleAllGroup() {
    ScaleStoreSnapshot scaleStoreSnapshot =
        ScaleStoreSnapshot.newBuilder()
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    2.396,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(2.053, 2.6848, 2.395, 3300, 3600, 20, 10)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    1.29,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(1.176, 1.52, 1.28, 3000, 3600, 20, 10)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    2.79,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(2.31, 2.87, 2.78, 2700, 3600, 20, 10),
                    }))
            .build();

    // Test with normal load conditions
    double load = 1.1; // 80% capacity utilization
    long lastModifyNano = ticker.read();
    Duration currentValue = Duration.ofDays(1);
    Duration result =
        reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
            scaleStoreSnapshot, load, lastModifyNano, currentValue);

    assertNotNull(result, "Result should not be null");
    Duration expected = currentValue.minus(Duration.ofMinutes(15));
    assertEquals(expected, result, "Should return reduced value for heavy load");
  }

  @Test
  public void testReduceDownScaleWindowDurationNoCandidate() {
    ScaleStoreSnapshot scaleStoreSnapshot =
        ScaleStoreSnapshot.newBuilder()
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    2.312,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(2.053, 2.6848, 2.395, 3300, 3600, 20, 10)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    1.2789,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(1.176, 1.52, 1.28, 3000, 3600, 20, 10)
                    }))
            .addJobGroupSnapshot(
                createJobGroupScaleStatusSnapshot(
                    2.678,
                    new ScaleComputerParams[] {
                      new ScaleComputerParams(2.31, 2.87, 2.78, 2700, 3600, 20, 10),
                    }))
            .build();

    // Test with normal load conditions
    double load = 1.1; // 80% capacity utilization
    long lastModifyNano = ticker.read();
    Duration currentValue = Duration.ofDays(1);
    Duration result =
        reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
            scaleStoreSnapshot, load, lastModifyNano, currentValue);

    assertNotNull(result, "Result should not be null");
    assertEquals(currentValue, result, "Should return reduced value for heavy load");
  }

  private JobGroupScaleStatusSnapshot createJobGroupScaleStatusSnapshot(
      double scale, ScaleComputerParams[] scaleComputerParams) {
    return JobGroupScaleStatusSnapshot.newBuilder()
        .setScaleStateSnapshot(
            ScaleStateSnapshot.newBuilder()
                .setScale(scale)
                .addAllScaleComputerSnapshots(
                    Arrays.stream(scaleComputerParams)
                        .map(this::createScaleComputerSnapshot)
                        .collect(Collectors.toList()))
                .build())
        .build();
  }

  private ScaleComputerSnapshot createScaleComputerSnapshot(ScaleComputerParams params) {
    return ScaleComputerSnapshot.newBuilder()
        .setWindowedComputerSnapshot(
            WindowedComputerSnapshot.newBuilder()
                .setLowerBoundary(params.lowerBoundary)
                .setUpperBoundary(params.upperBoundary)
                .setPercentileScale(params.percentileScale)
                .setWindowSnapshot(
                    WindowSnapshot.newBuilder()
                        .setSizeInSamples(params.sizeInSamples)
                        .setMinSizeInSamples(params.minSizeInSamples)
                        .setMinSizeInSeconds(params.minSizeInSeconds)
                        .setSizeInSeconds(params.sizeInSeconds))
                .build())
        .build();
  }

  private static class ScaleComputerParams {
    ScaleComputerParams(
        double lowerBoundary,
        double upperBoundary,
        double percentileScale,
        long sizeInSeconds,
        long minSizeInSeconds,
        int sizeInSamples,
        int minSizeInSamples) {
      this.lowerBoundary = lowerBoundary;
      this.upperBoundary = upperBoundary;
      this.percentileScale = percentileScale;
      this.sizeInSamples = sizeInSamples;
      this.minSizeInSamples = minSizeInSamples;
      this.minSizeInSeconds = minSizeInSeconds;
      this.sizeInSeconds = sizeInSeconds;
    }

    private final double lowerBoundary;
    private final double upperBoundary;
    private final double percentileScale;
    private final long sizeInSeconds;
    private final long minSizeInSeconds;
    private final int sizeInSamples;
    private final int minSizeInSamples;
  }
}
