package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.WindowSnapshot;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ScaleWindowTest extends FievelTestBase {
  private ScaleWindow scaleWindow;
  private TestUtils.TestTicker ticker;

  @Before
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
    Assert.assertTrue(isMature);
    Assert.assertEquals(sum / count, p50, 1);
    Assert.assertEquals(95, p90, 1);
  }

  @Test
  public void testMatureFalseBySamples() {
    for (double value = 1; value < 10; ++value) {
      ticker.add(Duration.ofSeconds(60));
      scaleWindow.add(1);
    }
    boolean isMature = scaleWindow.isMature();
    Assert.assertFalse(isMature);
  }

  @Test
  public void testMatureFalseByDuration() {
    for (double value = 0; value < 100; ++value) {
      ticker.add(Duration.ofSeconds(1));
      scaleWindow.add(1);
    }
    boolean isMature = scaleWindow.isMature();
    Assert.assertFalse(isMature);
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
    Assert.assertTrue(isMature);
    double result = scaleWindow.getByPercentile(0.99);
    Assert.assertTrue(result == 0.0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSamples() {
    ScaleWindow.newBuilder().withMinSamples(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBuckets() {
    ScaleWindow.newBuilder().withNBuckets(-1);
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
    Assert.assertTrue(scaleWindow.isMature());
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
    Assert.assertEquals((int) (5 * 60 * (1 - jitter)), minSizeInSeconds);
    // ensure jitter doesn't impact linear model of window size limit
    long offsetSeconds = 60;
    long offsetNano = TimeUnit.SECONDS.toNanos(offsetSeconds);
    supplier.durationNano = supplier.durationNano - offsetNano;
    snapshot = scaleWindow.snapshot();
    Assert.assertEquals(minSizeInSeconds - offsetSeconds, snapshot.getMinSizeInSeconds());
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
