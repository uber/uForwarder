package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.VegasLimit;
import com.netflix.concurrency.limits.limit.WindowedLimit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import java.util.Optional;

/** Adaptive inflight limiting implemented with Vegas limit */
public class VegasAdaptiveInflightLimiter extends AdaptiveInflightLimiter {
  private static final int CONCURRENCY_LIMITER_INITIAL_LIMIT = 100;
  private volatile SimpleLimiter limiter;
  private volatile AdaptiveMetrics metrics;

  private VegasAdaptiveInflightLimiter() {
    this.limiter = buildLimiter(CONCURRENCY_LIMITER_INITIAL_LIMIT);
    this.metrics = new AdaptiveMetrics(limiter);
  }

  @Override
  public void setMaxInflight(int maxInflight) {
    this.limiter = buildLimiter(maxInflight);
    this.metrics = new AdaptiveMetrics(limiter);
  }

  @Override
  Optional<Limiter.Listener> tryAcquireImpl() {
    return limiter.acquire(null);
  }

  @Override
  public InflightLimiter.Metrics getMetrics() {
    return metrics;
  }

  /**
   * Creates builder
   *
   * @return builder of VegasAdaptiveInflightLimiter
   */
  public static AdaptiveInflightLimiter.Builder newBuilder() {
    return () -> new VegasAdaptiveInflightLimiter();
  }

  private static SimpleLimiter buildLimiter(int maxInflight) {
    VegasLimit vegasLimit =
        VegasLimit.newBuilder()
            .initialLimit(CONCURRENCY_LIMITER_INITIAL_LIMIT)
            .maxConcurrency(maxInflight)
            .build();
    WindowedLimit limit = WindowedLimit.newBuilder().build(vegasLimit);
    return SimpleLimiter.newBuilder().limit(limit).build();
  }

  private class AdaptiveMetrics extends AdaptiveInflightLimiter.Metrics {
    private final SimpleLimiter limiter;

    private AdaptiveMetrics(SimpleLimiter limiter) {
      this.limiter = limiter;
    }

    @Override
    public long getInflight() {
      return limiter.getInflight();
    }

    @Override
    public long getLimit() {
      return limiter.getLimit();
    }
  }
}
