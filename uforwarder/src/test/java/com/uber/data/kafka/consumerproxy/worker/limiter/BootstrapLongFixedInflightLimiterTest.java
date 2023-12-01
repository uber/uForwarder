package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BootstrapLongFixedInflightLimiterTest extends FievelTestBase {
  private BootstrapLongFixedInflightLimiter limiter;

  @Before
  public void setUp() {
    limiter =
        BootstrapLongFixedInflightLimiter.newBuilder()
            .withBootstrapLimit(4)
            .withBootstrapCompleteThreshold(2)
            .withLimit(0)
            .build();
  }

  @Test
  public void testAcquireInBootstrap() {
    Assert.assertTrue(limiter.isBootstrapping());
    int succeed = 0;
    for (int i = 0; i < 10; ++i) {
      if (limiter.tryAcquire().isPresent()) {
        succeed++;
      }
    }
    Assert.assertEquals(4, succeed);
  }

  @Test
  public void testSwitchToWorking() {
    Assert.assertTrue(limiter.isBootstrapping());
    switchToWorking();
    Assert.assertFalse(limiter.isBootstrapping());
  }

  @Test
  public void testAcquireWorking() {
    Assert.assertTrue(limiter.isBootstrapping());
    switchToWorking();
    int succeed = 0;
    for (int i = 0; i < 10; ++i) {
      if (limiter.tryAcquire().isPresent()) {
        succeed++;
      }
    }
    Assert.assertEquals(10, succeed);
  }

  @Test
  public void testLowLimitLimiter() {
    BootstrapLongFixedInflightLimiter lowLimitLimiter =
        BootstrapLongFixedInflightLimiter.newBuilder()
            .withBootstrapLimit(4)
            .withBootstrapCompleteThreshold(2)
            .withLimit(1)
            .build();
    int succeed = 0;
    for (int i = 0; i < 10; ++i) {
      if (lowLimitLimiter.tryAcquire().isPresent()) {
        succeed++;
      }
    }
    Assert.assertEquals(1, succeed);
    lowLimitLimiter.updateLimit(10);
    for (int i = 0; i < 10; ++i) {
      if (lowLimitLimiter.tryAcquire().isPresent()) {
        succeed++;
      }
    }
    Assert.assertEquals(4, succeed);
  }

  private void switchToWorking() {
    for (int i = 0; i < 3; ++i) {
      limiter.tryAcquire().get().complete();
    }
  }
}
