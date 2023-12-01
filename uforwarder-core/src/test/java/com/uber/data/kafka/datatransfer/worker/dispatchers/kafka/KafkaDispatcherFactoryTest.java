package com.uber.data.kafka.datatransfer.worker.dispatchers.kafka;

import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Test;
import org.springframework.util.Assert;

public class KafkaDispatcherFactoryTest extends FievelTestBase {

  @Test
  public void testCreate() throws Exception {
    KafkaDispatcherFactory factory = new KafkaDispatcherFactory(new KafkaDispatcherConfiguration());
    KafkaDispatcher dispatcher =
        factory.create("test-client", "test-cluster", CoreInfra.NOOP, false, false);
    Assert.notNull(dispatcher, "dispatcher is created");

    dispatcher = factory.create("test-client", "test-cluster", CoreInfra.NOOP, false, true);
    Assert.notNull(dispatcher, "dispatcher is created");
  }
}
