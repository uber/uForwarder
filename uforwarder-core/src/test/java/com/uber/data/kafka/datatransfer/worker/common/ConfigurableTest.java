package com.uber.data.kafka.datatransfer.worker.common;

import org.junit.jupiter.api.Test;

public class ConfigurableTest {
  @Test
  public void testSetPipelineConfigManager() {
    Configurable configurable = new Configurable() {};
    configurable.setPipelineStateManager(new PipelineStateManager() {});
  }
}
