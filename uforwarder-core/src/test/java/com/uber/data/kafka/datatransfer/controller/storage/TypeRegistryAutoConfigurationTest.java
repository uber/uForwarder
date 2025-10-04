package com.uber.data.kafka.datatransfer.controller.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TypeRegistryAutoConfigurationTest {
  TypeRegistryAutoConfiguration typeRegistryAutoConfiguration = new TypeRegistryAutoConfiguration();

  @Test
  public void testTypeRegistry() {
    Assertions.assertNotNull(typeRegistryAutoConfiguration.typeRegistry());
  }
}
