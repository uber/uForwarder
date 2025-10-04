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
      JobStatusStoreConfiguration.JobStatusStoreConfigurationOverride.class,
      JobStatusStoreConfiguration.class
    })
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class JobStatusStoreConfigurationTest extends FievelTestBase {
  @Autowired JobStatusStoreConfiguration jobStatusStoreConfiguration;

  @Test
  public void test() {
    Assert.assertEquals(
        Duration.ofSeconds(20), jobStatusStoreConfiguration.getBufferedWriteInterval());
    Assert.assertEquals(Duration.ofMinutes(5), jobStatusStoreConfiguration.getTtl());
    Assert.assertEquals("/jobstatus/{id}", jobStatusStoreConfiguration.getZkDataPath());
    Assert.assertEquals("/sequencer/jobstatus", jobStatusStoreConfiguration.getZkSequencerPath());
  }
}
