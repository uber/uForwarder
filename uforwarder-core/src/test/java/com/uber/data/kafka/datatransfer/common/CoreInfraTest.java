package com.uber.data.kafka.datatransfer.common;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class CoreInfraTest extends FievelTestBase {

  @Test
  public void testGetDefaultRegion() {
    Assert.assertEquals("_region", CoreInfra.NOOP.getPlacement().getRegion());
  }

  @Test
  public void testGetZone() {
    Assert.assertEquals("_zone", CoreInfra.NOOP.getPlacement().getZone());
  }

  @Test
  public void getThreadMXBean() {
    Assert.assertNotNull(CoreInfra.NOOP.getThreadMXBean());
  }
}
