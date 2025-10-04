package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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
            .build(70, 100);
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
            .build(0, 0.0001);
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
}
