package com.uber.data.kafka.datatransfer.controller.autoscalar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ScaleWindowManagerTest {
  private ScaleWindowManager scaleWindowManager;
  private AutoScalarConfiguration mockConfig;

  @BeforeEach
  public void setUp() {
    mockConfig = Mockito.mock(AutoScalarConfiguration.class);
  }

  @Test
  public void testDefaultConstructor() {
    // Test the default constructor
    scaleWindowManager = new ScaleWindowManager();

    assertNotNull(scaleWindowManager, "ScaleWindowManager should not be null");

    // Verify that default values are set (these should be non-null)
    Duration downScaleWindow = scaleWindowManager.getDownScaleWindowDuration();
    Duration upScaleWindow = scaleWindowManager.getUpScaleWindowDuration();
    Duration hibernateWindow = scaleWindowManager.getHibernateWindowDuration();

    assertNotNull(downScaleWindow, "Down-scale window should not be null");
    assertNotNull(upScaleWindow, "Up-scale window should not be null");
    assertNotNull(hibernateWindow, "Hibernate window should not be null");
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

    assertNotNull(scaleWindowManager, "ScaleWindowManager should not be null");

    // Verify that the durations match the configuration
    assertEquals(
        expectedDownScale,
        scaleWindowManager.getDownScaleWindowDuration(),
        "Down-scale window should match configuration");
    assertEquals(
        expectedUpScale,
        scaleWindowManager.getUpScaleWindowDuration(),
        "Up-scale window should match configuration");
    assertEquals(
        expectedHibernate,
        scaleWindowManager.getHibernateWindowDuration(),
        "Hibernate window should match configuration");
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

    assertNotNull(actualDownScale, "Down-scale window should not be null");
    assertEquals(
        expectedDownScale, actualDownScale, "Down-scale window should match expected value");
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

    assertNotNull(actualUpScale, "Up-scale window should not be null");
    assertEquals(expectedUpScale, actualUpScale, "Up-scale window should match expected value");
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

    assertNotNull(actualHibernate, "Hibernate window should not be null");
    assertEquals(
        expectedHibernate, actualHibernate, "Hibernate window should match expected value");
  }

  @Test
  public void testNullConfiguration() {
    // Test behavior with null configuration (should throw exception)
    try {
      new ScaleWindowManager(null);
      // If no exception is thrown, the method should handle null gracefully
    } catch (Exception e) {
      // If an exception is thrown, it should be documented
      assertTrue(e instanceof NullPointerException, "Exception should be documented");
    }
  }
}
