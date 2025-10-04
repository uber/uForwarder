package com.uber.data.kafka.datatransfer.controller.autoscalar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class ReactiveScaleWindowCalculatorTest extends FievelTestBase {
  private ReactiveScaleWindowCalculator reactiveScaleWindowCalculator;

  @Before
  public void setUp() {
    reactiveScaleWindowCalculator = new ReactiveScaleWindowCalculator();
  }

  @Test
  public void testCalculateDownScaleWindowDuration() {
    // Test with normal load conditions
    double load = 0.8; // 80% capacity utilization
    long lastModifyNano = System.nanoTime();
    Duration currentValue = Duration.ofMinutes(5);

    Duration result =
        reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
            load, lastModifyNano, currentValue);

    assertNotNull("Result should not be null", result);
    assertEquals("Should return current value for normal load", currentValue, result);
  }
}
