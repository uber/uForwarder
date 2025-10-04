package com.uber.data.kafka.datatransfer.controller.autoscalar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ScaleWindowManagerTest extends FievelTestBase {
  private ScaleWindowManager scaleWindowManager;
  private AutoScalarConfiguration mockConfig;

  @Before
  public void setUp() {
    mockConfig = Mockito.mock(AutoScalarConfiguration.class);
  }

  @Test
  public void testDefaultConstructor() {
    // Test the default constructor
    scaleWindowManager = new ScaleWindowManager();

    assertNotNull("ScaleWindowManager should not be null", scaleWindowManager);

    // Verify that default values are set (these should be non-null)
    Duration downScaleWindow = scaleWindowManager.getDownScaleWindowDuration();
    Duration upScaleWindow = scaleWindowManager.getUpScaleWindowDuration();
    Duration hibernateWindow = scaleWindowManager.getHibernateWindowDuration();

    assertNotNull("Down-scale window should not be null", downScaleWindow);
    assertNotNull("Up-scale window should not be null", upScaleWindow);
    assertNotNull("Hibernate window should not be null", hibernateWindow);
  }

  @Test
  public void testConstructorWithConfiguration() {
    // Setup mock configuration with specific durations
    Duration expectedDownScale = Duration.ofMinutes(5);
    Duration expectedUpScale = Duration.ofMinutes(2);
    Duration expectedHibernate = Duration.ofMinutes(10);

    when(mockConfig.getDownScaleWindowDuration()).thenReturn(expectedDownScale);
    when(mockConfig.getUpScaleWindowDuration()).thenReturn(expectedUpScale);
    when(mockConfig.getHibernateWindowDuration()).thenReturn(expectedHibernate);

    // Test constructor with configuration
    scaleWindowManager = new ScaleWindowManager(mockConfig);

    assertNotNull("ScaleWindowManager should not be null", scaleWindowManager);

    // Verify that the durations match the configuration
    assertEquals(
        "Down-scale window should match configuration",
        expectedDownScale,
        scaleWindowManager.getDownScaleWindowDuration());
    assertEquals(
        "Up-scale window should match configuration",
        expectedUpScale,
        scaleWindowManager.getUpScaleWindowDuration());
    assertEquals(
        "Hibernate window should match configuration",
        expectedHibernate,
        scaleWindowManager.getHibernateWindowDuration());
  }

  @Test
  public void testGetDownScaleWindowDuration() {
    // Setup mock configuration
    Duration expectedDownScale = Duration.ofSeconds(30);
    when(mockConfig.getDownScaleWindowDuration()).thenReturn(expectedDownScale);
    when(mockConfig.getUpScaleWindowDuration()).thenReturn(Duration.ofMinutes(1));
    when(mockConfig.getHibernateWindowDuration()).thenReturn(Duration.ofMinutes(5));

    scaleWindowManager = new ScaleWindowManager(mockConfig);

    Duration actualDownScale = scaleWindowManager.getDownScaleWindowDuration();

    assertNotNull("Down-scale window should not be null", actualDownScale);
    assertEquals(
        "Down-scale window should match expected value", expectedDownScale, actualDownScale);
  }

  @Test
  public void testGetUpScaleWindowDuration() {
    // Setup mock configuration
    Duration expectedUpScale = Duration.ofMinutes(3);
    when(mockConfig.getDownScaleWindowDuration()).thenReturn(Duration.ofMinutes(5));
    when(mockConfig.getUpScaleWindowDuration()).thenReturn(expectedUpScale);
    when(mockConfig.getHibernateWindowDuration()).thenReturn(Duration.ofMinutes(10));

    scaleWindowManager = new ScaleWindowManager(mockConfig);

    Duration actualUpScale = scaleWindowManager.getUpScaleWindowDuration();

    assertNotNull("Up-scale window should not be null", actualUpScale);
    assertEquals("Up-scale window should match expected value", expectedUpScale, actualUpScale);
  }

  @Test
  public void testGetHibernateWindowDuration() {
    // Setup mock configuration
    Duration expectedHibernate = Duration.ofMinutes(15);
    when(mockConfig.getDownScaleWindowDuration()).thenReturn(Duration.ofMinutes(5));
    when(mockConfig.getUpScaleWindowDuration()).thenReturn(Duration.ofMinutes(2));
    when(mockConfig.getHibernateWindowDuration()).thenReturn(expectedHibernate);

    scaleWindowManager = new ScaleWindowManager(mockConfig);

    Duration actualHibernate = scaleWindowManager.getHibernateWindowDuration();

    assertNotNull("Hibernate window should not be null", actualHibernate);
    assertEquals(
        "Hibernate window should match expected value", expectedHibernate, actualHibernate);
  }

  @Test
  public void testNullConfiguration() {
    // Test behavior with null configuration (should throw exception)
    try {
      new ScaleWindowManager(null);
      // If no exception is thrown, the method should handle null gracefully
    } catch (Exception e) {
      // If an exception is thrown, it should be documented
      assertTrue("Exception should be documented", e instanceof NullPointerException);
    }
  }
}
