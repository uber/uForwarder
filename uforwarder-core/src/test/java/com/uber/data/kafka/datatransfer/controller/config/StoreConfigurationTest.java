package com.uber.data.kafka.datatransfer.controller.config;

import com.uber.data.kafka.datatransfer.controller.storage.Mode;
import com.uber.data.kafka.datatransfer.controller.storage.SerializerType;
import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {StoreConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class StoreConfigurationTest extends FievelTestBase {
  @Autowired StoreConfiguration storeConfiguration;

  @Test
  public void test() {
    Assert.assertEquals(Mode.ZK, storeConfiguration.getMode());
    Assert.assertEquals(SerializerType.JSON, storeConfiguration.getSerializerType());
    Assert.assertEquals(Duration.ZERO, storeConfiguration.getTtl());
    Assert.assertEquals(Duration.ZERO, storeConfiguration.getBufferedWriteInterval());
    IllegalStateException exception = null;
    try {
      storeConfiguration.getZkDataPath();
    } catch (IllegalStateException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);

    exception = null;
    try {
      storeConfiguration.getZkSequencerPath();
    } catch (IllegalStateException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }

  @Test
  public void testDefaultSerializer() {
    StoreConfiguration configuration = new StoreConfiguration();
    Assert.assertEquals(SerializerType.JSON, configuration.getSerializerType());
  }

  @Test
  public void testLogDecoratorEnabledByDefault() {
    StoreConfiguration configuration = new StoreConfiguration();
    Assert.assertTrue(configuration.isLogDecoratorEnabled());
  }
}
