package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.common.utils.PodIsolationStatus;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;

public class DynamicConfigurationTest extends FievelTestBase {
  @Test
  public void test() throws Exception {
    Assert.assertEquals(
        DynamicConfiguration.DEFAULT.getPodIsolationStatus(), PodIsolationStatus.DISABLED);
    Assert.assertTrue(DynamicConfiguration.DEFAULT.isOffsetCommittingEnabled());
    Assert.assertFalse(DynamicConfiguration.DEFAULT.isZoneIsolationDisabled());
    Assert.assertFalse(DynamicConfiguration.DEFAULT.isHeaderAllowed(new HashMap<>()));
    Assert.assertFalse(DynamicConfiguration.DEFAULT.isNettyEnabled(new HashMap<>()));
    Assert.assertFalse(
        DynamicConfiguration.DEFAULT.isAuthClientInterceptorEnabled(new HashMap<>()));
  }
}
