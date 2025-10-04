package com.uber.data.kafka.datatransfer.controller.autoscalar;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.data.kafka.datatransfer.WindowSnapshot;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScaleWindowTest {
  private ScaleWindow scaleWindow;
  private TestUtils.TestTicker ticker;

  @BeforeEach
  public void setup() {
    ticker = new TestUtils.TestTicker();
    scaleWindow =
        ScaleWindow.newBuilder()
            .withWindowDurationSupplier(() -> TimeUnit.MINUTES.toNanos(5))
            .withMinSamples(10)
            .withTicker(ticker)
            .withNBuckets(100)
            .build(70, 100, 0.0);
  }

  @Test
  public void testAddAndGetPercentile() {
    double sum = 0;
    double count = 0;
    for (double value = 60; value < 100; ++value) {
      ticker.add(Duration.ofSeconds(30));
      scaleWindow.add(value);
      sum += value;
      count++;
    }
    boolean isMature = scaleWindow.isMature();
    double p50 = scaleWindow.getByPercentile(0.5);
    double p90 = scaleWindow.getByPercentile(0.9);
    Assertions.assertTrue(isMature);
    Assertions.assertEquals(sum / count, p50, 1);
    Assertions.assertEquals(95, p90, 1);
  }

  @Test
  public void testMatureFalseBySamples() {
    for (double value = 1; value < 10; ++value) {
      ticker.add(Duration.ofSeconds(60));
      scaleWindow.add(1);
    }
    boolean isMature = scaleWindow.isMature();
    Assertions.assertFalse(isMature);
  }

  @Test
  public void testMatureFalseByDuration() {
    for (double value = 0; value < 100; ++value) {
      ticker.add(Duration.ofSeconds(1));
      scaleWindow.add(1);
    }
    boolean isMature = scaleWindow.isMature();
    Assertions.assertFalse(isMature);
  }

  @Test
  public void testGetP99Zero() {
    scaleWindow =
        ScaleWindow.newBuilder()
            .withWindowDurationSupplier(() -> TimeUnit.MINUTES.toNanos(5))
            .withMinSamples(10)
            .withTicker(ticker)
            .withNBuckets(100)
            .build(0, 0.0001, 0.0);
    for (double value = 0; value < 100; ++value) {
      ticker.add(Duration.ofSeconds(60));
      scaleWindow.add(0.0);
    }
    boolean isMature = scaleWindow.isMature();
    Assertions.assertTrue(isMature);
    double result = scaleWindow.getByPercentile(0.99);
    Assertions.assertTrue(result == 0.0);
  }

  @Test
  public void testInvalidSamples() {
    assertThrows(IllegalArgumentException.class, () -> ScaleWindow.newBuilder().withMinSamples(-1));
  }

  @Test
  public void testInvalidBuckets() {
    assertThrows(IllegalArgumentException.class, () -> ScaleWindow.newBuilder().withNBuckets(-1));
  }

  @Test
  public void testMatureWithJitter() {
    scaleWindow =
        ScaleWindow.newBuilder()
            .withWindowDurationSupplier(() -> TimeUnit.MINUTES.toNanos(5))
            .withMinSamples(5)
            .withTicker(ticker)
            .withNBuckets(100)
            .build(70, 100, 0.5);
    for (int i = 0; i < 6; ++i) {
      ticker.add(Duration.ofSeconds(30));
      scaleWindow.add(i);
    }
    Assertions.assertTrue(scaleWindow.isMature());
  }

  @Test
  public void testSnapshotWithJitter() {
    double jitter = 0.4;
    WindowDurationSupplier supplier = new WindowDurationSupplier(TimeUnit.MINUTES.toNanos(5));
    scaleWindow =
        ScaleWindow.newBuilder()
            .withWindowDurationSupplier(supplier)
            .withMinSamples(5)
            .withTicker(ticker)
            .withNBuckets(100)
            .build(70, 100, jitter);
    WindowSnapshot snapshot = scaleWindow.snapshot();
    long minSizeInSeconds = snapshot.getMinSizeInSeconds();
    Assertions.assertEquals((int) (5 * 60 * (1 - jitter)), minSizeInSeconds);
    // ensure jitter doesn't impact linear model of window size limit
    long offsetSeconds = 60;
    long offsetNano = TimeUnit.SECONDS.toNanos(offsetSeconds);
    supplier.durationNano = supplier.durationNano - offsetNano;
    snapshot = scaleWindow.snapshot();
    Assertions.assertEquals(minSizeInSeconds - offsetSeconds, snapshot.getMinSizeInSeconds());
  }

  private static class WindowDurationSupplier implements Supplier<Long> {
    private long durationNano;

    WindowDurationSupplier(long durationNano) {
      this.durationNano = durationNano;
    }

    @Override
    public Long get() {
      return durationNano;
    }
  }
}
