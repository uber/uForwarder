package com.uber.data.kafka.consumerproxy.worker.limiter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VegasAdaptiveInflightLimiterTest {
  @Test
  public void testAcquire() throws InterruptedException {
    AdaptiveInflightLimiter adaptiveInflightLimiter =
        VegasAdaptiveInflightLimiter.newBuilder().build();
    InflightLimiter.Permit p = adaptiveInflightLimiter.acquire();
    Assertions.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());
    p.complete();
    Assertions.assertEquals(0, adaptiveInflightLimiter.getMetrics().getInflight());
  }

  @Test
  public void testUpdateMaxLimit() throws InterruptedException {
    AdaptiveInflightLimiter adaptiveInflightLimiter =
        VegasAdaptiveInflightLimiter.newBuilder().build();
    InflightLimiter.Permit p = adaptiveInflightLimiter.acquire();
    Assertions.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());
    adaptiveInflightLimiter.setMaxInflight(100);
    Assertions.assertEquals(0, adaptiveInflightLimiter.getMetrics().getInflight());
  }
}
