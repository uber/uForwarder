package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WindowedAggregatorTest extends FievelTestBase {
  private WindowedAggregator<Long> aggregator;
  private TestUtils.TestTicker testTicker;

  @Before
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
    Assert.assertEquals(3L, result);

    testTicker.add(Duration.ofSeconds(66));
    result = aggregator.get(0L);
    Assert.assertEquals(0L, result);

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
    Assert.assertEquals(8L, result);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidDuration() {
    WindowedAggregator.newBuilder()
        .withTicker(testTicker)
        .withBucketDuration(-5, TimeUnit.SECONDS)
        .withNBuckets(12)
        .of((a, b) -> Math.max((int) a, (int) b));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidBucket() {
    WindowedAggregator.newBuilder()
        .withTicker(testTicker)
        .withBucketDuration(5, TimeUnit.SECONDS)
        .withNBuckets(-12)
        .of((a, b) -> Math.max((int) a, (int) b));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidAggregator() {
    WindowedAggregator.newBuilder()
        .withTicker(testTicker)
        .withBucketDuration(5, TimeUnit.SECONDS)
        .withNBuckets(12)
        .of(null);
  }
}
