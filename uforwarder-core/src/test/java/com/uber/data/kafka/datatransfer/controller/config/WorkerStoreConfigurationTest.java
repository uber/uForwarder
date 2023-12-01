package com.uber.data.kafka.datatransfer.controller.config;

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
@SpringBootTest(
  classes = {
    StoreConfiguration.class,
    WorkerStoreConfiguration.WorkerStoreConfigurationOverride.class,
    WorkerStoreConfiguration.class
  }
)
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class WorkerStoreConfigurationTest extends FievelTestBase {
  @Autowired WorkerStoreConfiguration workerStoreConfiguration;

  @Test
  public void test() {
    Assert.assertEquals(Duration.ofMinutes(2), workerStoreConfiguration.getTtl());
    Assert.assertEquals("/workers/{id}", workerStoreConfiguration.getZkDataPath());
    Assert.assertEquals("/sequencer/worker", workerStoreConfiguration.getZkSequencerPath());
    Assert.assertEquals(
        Duration.ofSeconds(30), workerStoreConfiguration.getBufferedWriteInterval());
  }
}
