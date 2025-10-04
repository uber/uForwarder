package com.uber.data.kafka.datatransfer.controller.autoscalar;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScaleStatusStoreTest {
  private ScaleStatusStore scaleStatusStore;
  private AutoScalarConfiguration autoScalarConfiguration;
  private TestUtils.TestTicker ticker;

  @BeforeEach
  public void setUp() {
    autoScalarConfiguration = new AutoScalarConfiguration();
    ticker = new TestUtils.TestTicker();
    scaleStatusStore = new ScaleStatusStore(autoScalarConfiguration, ticker);
  }

  @Test
  public void testConstructorWithConfigurationAndTicker() {
    // Test the two-parameter constructor
    ScaleStatusStore store = new ScaleStatusStore(autoScalarConfiguration, ticker);

    assertNotNull(store, "ScaleStatusStore should not be null");

    // Verify that we can access the underlying map
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = store.asMap();
    assertNotNull(map, "Map should not be null");
    assertTrue(map.isEmpty(), "Map should be empty initially");
  }

  @Test
  public void testConstructorWithConfigurationOnly() {
    // Test the single-parameter constructor
    ScaleStatusStore store = new ScaleStatusStore(autoScalarConfiguration);

    assertNotNull(store, "ScaleStatusStore should not be null");

    // Verify that we can access the underlying map
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = store.asMap();
    assertNotNull(map, "Map should not be null");
    assertTrue(map.isEmpty(), "Map should be empty initially");
  }

  @Test
  public void testAsMap() {
    // Test that asMap() returns a valid map
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = scaleStatusStore.asMap();

    assertNotNull(map, "Map should not be null");
    assertTrue(map.isEmpty(), "Map should be empty initially");

    // Test that the same map instance is returned (view of the cache)
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map2 = scaleStatusStore.asMap();
    assertSame(map, map2, "Should return the same map instance");
  }

  @Test
  public void testCleanUp() {
    // Test that cleanUp() doesn't throw exceptions
    // Since the cache is empty, cleanup should have no effect
    scaleStatusStore.cleanUp();

    // Verify that the store is still functional after cleanup
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = scaleStatusStore.asMap();
    assertNotNull(map, "Map should not be null after cleanup");
    assertTrue(map.isEmpty(), "Map should still be empty after cleanup");
  }

  @Test
  public void testMapViewConsistency() {
    // Test that the map view is consistent with the underlying cache
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map1 = scaleStatusStore.asMap();
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map2 = scaleStatusStore.asMap();

    // Both should be the same instance (view of the cache)
    assertSame(map1, map2, "Map views should be the same instance");

    // Both should reflect the same state
    assertEquals(map1.size(), map2.size(), "Map sizes should be equal");
    assertTrue(map1.isEmpty() && map2.isEmpty(), "Both maps should be empty");
  }

  @Test
  public void testCleanUpAfterMapAccess() {
    // Test cleanup after accessing the map
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = scaleStatusStore.asMap();
    assertNotNull(map, "Map should not be null");

    scaleStatusStore.cleanUp();

    // Map should still be accessible after cleanup
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> mapAfterCleanup = scaleStatusStore.asMap();
    assertNotNull(mapAfterCleanup, "Map should not be null after cleanup");
    assertSame(map, mapAfterCleanup, "Should return the same map instance");
  }
}
