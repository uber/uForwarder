package com.uber.data.kafka.consumerproxy.worker.limiter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VegasAdaptiveInlightLimiterTest {
  @Test
  public void testBuild() {
    VegasAdaptiveInflightLimiter.Builder builder = VegasAdaptiveInflightLimiter.newBuilder();
    AdaptiveInflightLimiter limiter = builder.build();
    Assertions.assertTrue(limiter.getMetrics().getExtraMetrics().isEmpty());
    Assertions.assertEquals(100, limiter.getMetrics().getLimit());
  }
}
