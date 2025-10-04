package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.common.utils.PodIsolationStatus;
import java.util.HashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DynamicConfigurationTest {
  @Test
  public void test() throws Exception {
    Assertions.assertEquals(
        DynamicConfiguration.DEFAULT.getPodIsolationStatus(), PodIsolationStatus.DISABLED);
    Assertions.assertTrue(DynamicConfiguration.DEFAULT.isOffsetCommittingEnabled());
    Assertions.assertFalse(DynamicConfiguration.DEFAULT.isZoneIsolationDisabled());
    Assertions.assertFalse(DynamicConfiguration.DEFAULT.isHeaderAllowed(new HashMap<>()));
    Assertions.assertFalse(DynamicConfiguration.DEFAULT.isNettyEnabled(new HashMap<>()));
    Assertions.assertFalse(
        DynamicConfiguration.DEFAULT.isAuthClientInterceptorEnabled(new HashMap<>()));
  }
}
