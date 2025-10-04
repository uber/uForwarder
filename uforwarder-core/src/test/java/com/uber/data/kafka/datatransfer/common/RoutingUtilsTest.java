package com.uber.data.kafka.datatransfer.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RoutingUtilsTest {

  @Test
  public void testExtractAddress() {
    Assertions.assertEquals("service-name", RoutingUtils.extractAddress("dns://service-name"));
    Assertions.assertEquals("unknown", RoutingUtils.extractAddress("bad-format"));
    Assertions.assertEquals("service-name", RoutingUtils.extractAddress("dns:///service-name"));
  }

  @Test
  public void testExtractAddressWithInvalidChar() {
    Assertions.assertEquals("address1", RoutingUtils.extractAddress("dns://address1@bad1@bad2"));
  }
}
