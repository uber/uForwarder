package com.uber.data.kafka.datatransfer.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CoreInfraTest {

  @Test
  public void testGetDefaultRegion() {
    Assertions.assertEquals("_region", CoreInfra.NOOP.getPlacement().getRegion());
  }

  @Test
  public void testGetZone() {
    Assertions.assertEquals("_zone", CoreInfra.NOOP.getPlacement().getZone());
  }

  @Test
  public void testGetThreadMXBean() {
    Assertions.assertNotNull(CoreInfra.NOOP.getThreadMXBean());
  }

  @Test
  public void testGetOperatingSystemMXBean() {
    Assertions.assertNotNull(CoreInfra.NOOP.getOperatingSystemMXBean());
  }
}
