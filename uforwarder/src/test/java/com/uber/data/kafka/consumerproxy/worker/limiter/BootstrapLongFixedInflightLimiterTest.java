package com.uber.data.kafka.consumerproxy.worker.limiter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BootstrapLongFixedInflightLimiterTest {
  private BootstrapLongFixedInflightLimiter limiter;

  @BeforeEach
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
    Assertions.assertTrue(limiter.isBootstrapping());
    int succeed = 0;
    for (int i = 0; i < 10; ++i) {
      if (limiter.tryAcquire().isPresent()) {
        succeed++;
      }
    }
    Assertions.assertEquals(4, succeed);
  }

  @Test
  public void testSwitchToWorking() {
    Assertions.assertTrue(limiter.isBootstrapping());
    switchToWorking();
    Assertions.assertFalse(limiter.isBootstrapping());
  }

  @Test
  public void testAcquireWorking() {
    Assertions.assertTrue(limiter.isBootstrapping());
    switchToWorking();
    int succeed = 0;
    for (int i = 0; i < 10; ++i) {
      if (limiter.tryAcquire().isPresent()) {
        succeed++;
      }
    }
    Assertions.assertEquals(10, succeed);
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
    Assertions.assertEquals(1, succeed);
    lowLimitLimiter.updateLimit(10);
    for (int i = 0; i < 10; ++i) {
      if (lowLimitLimiter.tryAcquire().isPresent()) {
        succeed++;
      }
    }
    Assertions.assertEquals(4, succeed);
  }

  private void switchToWorking() {
    for (int i = 0; i < 3; ++i) {
      limiter.tryAcquire().get().complete();
    }
  }
}
