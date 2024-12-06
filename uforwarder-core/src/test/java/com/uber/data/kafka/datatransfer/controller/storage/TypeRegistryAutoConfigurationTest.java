package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TypeRegistryAutoConfigurationTest extends FievelTestBase {
  TypeRegistryAutoConfiguration typeRegistryAutoConfiguration = new TypeRegistryAutoConfiguration();

  @Test
  public void testTypeRegistry() {
    Assert.assertNotNull(typeRegistryAutoConfiguration.typeRegistry());
  }
}
