package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Test;

public class ConfigurableTest extends FievelTestBase {
  @Test
  public void testSetPipelineConfigManager() {
    Configurable configurable = new Configurable() {};
    configurable.setPipelineStateManager(new PipelineStateManager() {});
  }
}
