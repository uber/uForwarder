package com.uber.data.kafka.datatransfer.controller.autoscalar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import com.uber.data.kafka.datatransfer.ScaleStoreSnapshot;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReactiveScaleWindowManagerTest extends FievelTestBase {
  private ReactiveScaleWindowManager reactiveScaleWindowManager;
  private ReactiveScaleWindowCalculator reactiveScaleWindowCalculator;
  private AutoScalarConfiguration autoScalingConfig;
  private TestUtils.TestTicker ticker;
  private ScaleStatusStore scaleStatusStore;

  @Before
  public void setUp() {

    reactiveScaleWindowCalculator = Mockito.mock(ReactiveScaleWindowCalculator.class);
    autoScalingConfig = new AutoScalarConfiguration();
    autoScalingConfig.setReactiveScaleWindowEnabled(true);
    ticker = new TestUtils.TestTicker();
    scaleStatusStore = new ScaleStatusStore(autoScalingConfig, ticker);
    reactiveScaleWindowManager =
        new ReactiveScaleWindowManager(
            scaleStatusStore, autoScalingConfig, ticker, reactiveScaleWindowCalculator);
  }

  @Test
  public void testConstructor() {
    // Test that the manager is properly constructed
    assertNotNull("ReactiveScaleWindowManager should not be null", reactiveScaleWindowManager);

    // Verify that we can access inherited methods
    Duration downScaleWindow = reactiveScaleWindowManager.getDownScaleWindowDuration();
    Duration upScaleWindow = reactiveScaleWindowManager.getUpScaleWindowDuration();
    Duration hibernateWindow = reactiveScaleWindowManager.getHibernateWindowDuration();

    assertNotNull("Down-scale window should not be null", downScaleWindow);
    assertNotNull("Up-scale window should not be null", upScaleWindow);
    assertNotNull("Hibernate window should not be null", hibernateWindow);
  }

  @Test
  public void testOnSampleWithNormalLoad() {
    // Test with normal load conditions
    double load = 0.8; // 80% capacity utilization

    // This should not throw any exceptions
    reactiveScaleWindowManager.onSample(load);

    // Verify that the manager is still functional after processing the sample
    Duration downScaleWindow = reactiveScaleWindowManager.getDownScaleWindowDuration();
    Assert.assertEquals(autoScalingConfig.getDownScaleWindowDuration(), downScaleWindow);
  }

  @Test
  public void testOnSampleWithReactiveCalculator() {
    // Test that the reactive calculator is called when appropriate
    double load = 0.8;
    Duration expectedNewDuration = Duration.ofMinutes(3);

    // Mock the calculator to return a specific duration
    when(reactiveScaleWindowCalculator.calculateDownScaleWindowDuration(
            Mockito.any(ScaleStoreSnapshot.class),
            Mockito.anyDouble(),
            Mockito.anyLong(),
            Mockito.any(Duration.class)))
        .thenReturn(expectedNewDuration);

    // Process multiple samples to potentially trigger reactive calculation
    for (int i = 0; i < 100; i++) {
      ticker.add(Duration.ofSeconds(4));
      reactiveScaleWindowManager.onSample(load);
    }

    // Verify that the manager remains functional
    Duration downScaleWindow = reactiveScaleWindowManager.getDownScaleWindowDuration();
    assertEquals(expectedNewDuration, downScaleWindow);
    Mockito.verify(reactiveScaleWindowCalculator, Mockito.times(1))
        .calculateDownScaleWindowDuration(
            Mockito.any(ScaleStoreSnapshot.class),
            Mockito.anyDouble(),
            Mockito.anyLong(),
            Mockito.any(Duration.class));

    // verify feature flag works
    autoScalingConfig.setReactiveScaleWindowEnabled(false);
    assertEquals(
        autoScalingConfig.getDownScaleWindowDuration(),
        reactiveScaleWindowManager.getDownScaleWindowDuration());
  }
}
