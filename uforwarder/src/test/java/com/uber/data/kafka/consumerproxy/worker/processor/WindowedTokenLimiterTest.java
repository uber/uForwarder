package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WindowedTokenLimiterTest {
  private WindowedTokenLimiter tokenLimiter;
  private TestTicker ticker;

  @BeforeEach
  public void setup() {
    ticker = new TestTicker();
    tokenLimiter =
        WindowedTokenLimiter.newBuilder()
            .withWindowMillis(50)
            .withDefaultTokens(0)
            .withTicker(ticker)
            .build();
  }

  @Test
  public void testAcquire() {
    boolean permitted = tokenLimiter.tryAcquire(1);
    Assertions.assertFalse(permitted);
    tokenLimiter.credit(1);

    ticker.add(TimeUnit.MILLISECONDS.toNanos(10));

    permitted = tokenLimiter.tryAcquire(1);
    Assertions.assertTrue(permitted);
  }

  @Test
  public void testGetTokens() {
    for (int i = 0; i < 100; ++i) {
      tokenLimiter.credit(1);
      Assertions.assertEquals(i < 50 ? i + 1 : 50, tokenLimiter.getMetrics().getNumTokens());
      ticker.add(TimeUnit.MILLISECONDS.toNanos(1));
    }
  }

  @Test
  public void testAcquireExpired() {
    tokenLimiter.credit(1);

    ticker.add(TimeUnit.MILLISECONDS.toNanos(100));
    boolean permitted = tokenLimiter.tryAcquire(1);
    Assertions.assertFalse(permitted);
  }

  private static class TestTicker extends Ticker {
    private long timeNano = System.nanoTime();

    @Override
    public long read() {
      return timeNano;
    }

    private void add(long elapsed) {
      timeNano += elapsed;
    }
  }
}
