package com.uber.data.kafka.datatransfer.common;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class RoutingUtilsTest extends FievelTestBase {

  @Test
  public void testExtractAddress() {
    Assert.assertEquals("service-name", RoutingUtils.extractAddress("dns://service-name"));
    Assert.assertEquals("unknown", RoutingUtils.extractAddress("bad-format"));
    Assert.assertEquals("service-name", RoutingUtils.extractAddress("dns:///service-name"));
  }

  @Test
  public void testExtractAddressWithInvalidChar() {
    Assert.assertEquals("address1", RoutingUtils.extractAddress("dns://address1@bad1@bad2"));
  }
}
