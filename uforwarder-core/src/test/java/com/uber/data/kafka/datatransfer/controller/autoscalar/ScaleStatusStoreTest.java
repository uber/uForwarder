package com.uber.data.kafka.datatransfer.controller.autoscalar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class ScaleStatusStoreTest extends FievelTestBase {
  private ScaleStatusStore scaleStatusStore;
  private AutoScalarConfiguration autoScalarConfiguration;
  private TestUtils.TestTicker ticker;

  @Before
  public void setUp() {
    autoScalarConfiguration = new AutoScalarConfiguration();
    ticker = new TestUtils.TestTicker();
    scaleStatusStore = new ScaleStatusStore(autoScalarConfiguration, ticker);
  }

  @Test
  public void testConstructorWithConfigurationAndTicker() {
    // Test the two-parameter constructor
    ScaleStatusStore store = new ScaleStatusStore(autoScalarConfiguration, ticker);

    assertNotNull("ScaleStatusStore should not be null", store);

    // Verify that we can access the underlying map
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = store.asMap();
    assertNotNull("Map should not be null", map);
    assertTrue("Map should be empty initially", map.isEmpty());
  }

  @Test
  public void testConstructorWithConfigurationOnly() {
    // Test the single-parameter constructor
    ScaleStatusStore store = new ScaleStatusStore(autoScalarConfiguration);

    assertNotNull("ScaleStatusStore should not be null", store);

    // Verify that we can access the underlying map
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = store.asMap();
    assertNotNull("Map should not be null", map);
    assertTrue("Map should be empty initially", map.isEmpty());
  }

  @Test
  public void testAsMap() {
    // Test that asMap() returns a valid map
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = scaleStatusStore.asMap();

    assertNotNull("Map should not be null", map);
    assertTrue("Map should be empty initially", map.isEmpty());

    // Test that the same map instance is returned (view of the cache)
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map2 = scaleStatusStore.asMap();
    assertSame("Should return the same map instance", map, map2);
  }

  @Test
  public void testCleanUp() {
    // Test that cleanUp() doesn't throw exceptions
    // Since the cache is empty, cleanup should have no effect
    scaleStatusStore.cleanUp();

    // Verify that the store is still functional after cleanup
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = scaleStatusStore.asMap();
    assertNotNull("Map should not be null after cleanup", map);
    assertTrue("Map should still be empty after cleanup", map.isEmpty());
  }

  @Test
  public void testMapViewConsistency() {
    // Test that the map view is consistent with the underlying cache
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map1 = scaleStatusStore.asMap();
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map2 = scaleStatusStore.asMap();

    // Both should be the same instance (view of the cache)
    assertSame("Map views should be the same instance", map1, map2);

    // Both should reflect the same state
    assertEquals("Map sizes should be equal", map1.size(), map2.size());
    assertTrue("Both maps should be empty", map1.isEmpty() && map2.isEmpty());
  }

  @Test
  public void testCleanUpAfterMapAccess() {
    // Test cleanup after accessing the map
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> map = scaleStatusStore.asMap();
    assertNotNull("Map should not be null", map);

    scaleStatusStore.cleanUp();

    // Map should still be accessible after cleanup
    Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> mapAfterCleanup = scaleStatusStore.asMap();
    assertNotNull("Map should not be null after cleanup", mapAfterCleanup);
    assertSame("Should return the same map instance", map, mapAfterCleanup);
  }
}
