package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class VegasAdaptiveInlightLimiterTest extends FievelTestBase {
  @Test
  public void testBuild() {
    VegasAdaptiveInflightLimiter.Builder builder = VegasAdaptiveInflightLimiter.newBuilder();
    AdaptiveInflightLimiter limiter = builder.build();
    Assert.assertTrue(limiter.getMetrics().getExtraMetrics().isEmpty());
    Assert.assertEquals(100, limiter.getMetrics().getLimit());
  }
}
