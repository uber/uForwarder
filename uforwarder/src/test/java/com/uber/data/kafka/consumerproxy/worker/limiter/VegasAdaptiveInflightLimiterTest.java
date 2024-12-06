package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class VegasAdaptiveInflightLimiterTest extends FievelTestBase {
  @Test
  public void testAcquire() throws InterruptedException {
    AdaptiveInflightLimiter adaptiveInflightLimiter =
        VegasAdaptiveInflightLimiter.newBuilder().build();
    InflightLimiter.Permit p = adaptiveInflightLimiter.acquire();
    Assert.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());
    p.complete();
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getInflight());
  }

  @Test
  public void testUpdateMaxLimit() throws InterruptedException {
    AdaptiveInflightLimiter adaptiveInflightLimiter =
        VegasAdaptiveInflightLimiter.newBuilder().build();
    InflightLimiter.Permit p = adaptiveInflightLimiter.acquire();
    Assert.assertEquals(1, adaptiveInflightLimiter.getMetrics().getInflight());
    adaptiveInflightLimiter.setMaxInflight(100);
    Assert.assertEquals(0, adaptiveInflightLimiter.getMetrics().getInflight());
  }
}
