package com.uber.data.kafka.consumerproxy.worker.limiter;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WindowedAggregatorTest {
  private WindowedAggregator<Long> aggregator;
  private TestUtils.TestTicker testTicker;

  @BeforeEach
  public void setup() {
    this.testTicker = new TestUtils.TestTicker();
    this.aggregator =
        WindowedAggregator.newBuilder()
            .withTicker(testTicker)
            .withBucketDuration(5, TimeUnit.SECONDS)
            .withNBuckets(12)
            .of((a, b) -> Math.max(a, b));
  }

  @Test
  public void testAggregator() {
    aggregator.put(3L);
    long result = aggregator.get(0L);
    Assertions.assertEquals(3L, result);

    testTicker.add(Duration.ofSeconds(66));
    result = aggregator.get(0L);
    Assertions.assertEquals(0L, result);

    aggregator.put(10L);
    testTicker.add(Duration.ofSeconds(12));

    aggregator.put(9L);
    testTicker.add(Duration.ofSeconds(12));

    aggregator.put(8L);
    testTicker.add(Duration.ofSeconds(12));

    aggregator.put(7L);
    testTicker.add(Duration.ofSeconds(12));

    aggregator.put(6L);
    testTicker.add(Duration.ofSeconds(12));

    aggregator.put(5L);
    testTicker.add(Duration.ofSeconds(12));

    result = aggregator.get(0L);
    Assertions.assertEquals(8L, result);
  }

  @Test
  public void testInvalidDuration() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WindowedAggregator.newBuilder()
                .withTicker(testTicker)
                .withBucketDuration(-5, TimeUnit.SECONDS)
                .withNBuckets(12)
                .of((a, b) -> Math.max((int) a, (int) b)));
  }

  @Test
  public void testInvalidBucket() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WindowedAggregator.newBuilder()
                .withTicker(testTicker)
                .withBucketDuration(5, TimeUnit.SECONDS)
                .withNBuckets(-12)
                .of((a, b) -> Math.max((int) a, (int) b)));
  }

  @Test
  public void testInvalidAggregator() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            WindowedAggregator.newBuilder()
                .withTicker(testTicker)
                .withBucketDuration(5, TimeUnit.SECONDS)
                .withNBuckets(12)
                .of(null));
  }
}
