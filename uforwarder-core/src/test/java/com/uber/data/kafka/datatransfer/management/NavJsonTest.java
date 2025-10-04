package com.uber.data.kafka.datatransfer.management;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NavJsonTest {
  @Test
  public void testRead() throws Exception {
    Assertions.assertNotNull(new NavJson("service", "host", "master").read());
  }
}
