package com.uber.data.kafka.datatransfer.controller.config;

import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {StoreConfiguration.class, JobGroupStoreConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class JobGroupStoreConfigurationTest extends FievelTestBase {
  @Autowired JobGroupStoreConfiguration configuration;

  @Test
  public void test() {
    Assert.assertEquals("/jobgroups/{id}", configuration.getZkDataPath());
    Assert.assertEquals("/sequencer/jobgroup", configuration.getZkSequencerPath());
  }
}
