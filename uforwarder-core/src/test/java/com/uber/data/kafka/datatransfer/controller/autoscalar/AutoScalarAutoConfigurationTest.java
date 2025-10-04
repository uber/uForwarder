package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rpc.JobWorkloadSink;
import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(
    classes = {
      AutoScalarAutoConfiguration.class,
    })
@ActiveProfiles(profiles = {"data-transfer-controller"})
@TestPropertySource(
    properties = {"spring.config.location=classpath:/base.yaml", "master.store.enabled=false"})
public class AutoScalarAutoConfigurationTest extends FievelTestBase {
  @MockBean LeaderSelector leaderSelector;
  @Autowired ScaleStatusStore scaleStatusStore;
  @Autowired JobWorkloadSink jobWorkloadSink;
  @Autowired ReactiveScaleWindowCalculator reactiveScaleWindowCalculator;
  @Autowired ReactiveScaleWindowManager scaleWindowManager;
  @Autowired AutoScalar autoScaler;

  @Test
  public void test() {
    Assert.assertNotNull(scaleStatusStore);
    Assert.assertNotNull(jobWorkloadSink);
    Assert.assertNotNull(reactiveScaleWindowCalculator);
    Assert.assertNotNull(scaleWindowManager);
    Assert.assertNotNull(autoScaler);
  }
}
