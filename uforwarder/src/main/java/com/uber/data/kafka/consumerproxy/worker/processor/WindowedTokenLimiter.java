package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.base.Ticker;
import com.uber.concurrency.loadbalancer.timedcounter.WindowScheduledCounter;
import com.uber.concurrency.loadbalancer.timedcounter.WindowTimedCounter;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/** WindowedTokenLimiter implements {@link TokenLimiter} with time windowed bucket */
public class WindowedTokenLimiter implements TokenLimiter {
  private final WindowTimedCounter timedCounter; // time window buckets
  private final long defaultTokens; // default number of tokens grant to the limiter
  private final MetricsImpl metrics;

  private WindowedTokenLimiter(Builder builder) {
    this.defaultTokens = builder.defaultTokens;
    this.timedCounter =
        new WindowTimedCounter(
            new WindowScheduledCounter.Builder()
                .withTicker(builder.ticker)
                .withMaxDelay(Duration.ofMillis(builder.windowMillis)));
    this.metrics = new MetricsImpl();
  }

  @Override
  public void credit(int n) {
    timedCounter.add(n);
  }

  @Override
  public boolean tryAcquire(int n) {
    timedCounter.check();
    long nTokens = timedCounter.get();
    if (nTokens + defaultTokens < n) {
      return false;
    }
    timedCounter.add(-n);
    return true;
  }

  @Override
  public Metrics getMetrics() {
    return metrics;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public class MetricsImpl implements Metrics {

    @Override
    public int getNumTokens() {
      timedCounter.check();
      return (int)
          Math.max(
              Integer.MIN_VALUE, Math.min(Integer.MAX_VALUE, timedCounter.get() + defaultTokens));
    }
  }

  public static class Builder {
    private long defaultTokens = 0;
    private long windowMillis = TimeUnit.MINUTES.toMillis(5);
    private Ticker ticker = Ticker.systemTicker();

    private Builder() {}

    /**
     * Sets default number of tokens in the limiter
     *
     * @param defaultTokens the default number of tokens
     * @return the builder
     */
    public Builder withDefaultTokens(long defaultTokens) {
      this.defaultTokens = defaultTokens;
      return this;
    }

    /**
     * Sets window duration in millis, tokens expires after the duration
     *
     * @param windowMillis the window length in milliseconds
     * @return the builder
     */
    public Builder withWindowMillis(long windowMillis) {
      this.windowMillis = windowMillis;
      return this;
    }

    public Builder withTicker(Ticker ticker) {
      this.ticker = ticker;
      return this;
    }

    public WindowedTokenLimiter build() {
      return new WindowedTokenLimiter(this);
    }
  }
}
