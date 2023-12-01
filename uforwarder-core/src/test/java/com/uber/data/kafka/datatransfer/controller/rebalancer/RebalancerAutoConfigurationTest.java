package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

public class RebalancerAutoConfigurationTest extends FievelTestBase {

  @Test
  public void testInitialized() {
    new ApplicationContextRunner()
        .withAllowBeanDefinitionOverriding(true)
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withConfiguration(AutoConfigurations.of(RebalancerAutoConfiguration.class))
        .withSystemProperties("spring.config.location=classpath:/base.yaml")
        .withPropertyValues("spring.profiles.active=data-transfer-controller")
        .run(
            context -> {
              Rebalancer rebalancer = context.getBean(Rebalancer.class);
              Assert.assertNotNull(rebalancer);
              Assert.assertFalse(
                  rebalancer instanceof RebalancerAutoConfiguration2.SimpleRebalancer);
            });
  }

  @Test
  public void testInitializedNoConflict() {
    new ApplicationContextRunner()
        .withAllowBeanDefinitionOverriding(true)
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withConfiguration(
            AutoConfigurations.of(
                RebalancerAutoConfiguration.class, RebalancerAutoConfiguration2.class))
        .withSystemProperties("spring.config.location=classpath:/base.yaml")
        .withPropertyValues("spring.profiles.active=data-transfer-controller")
        .run(
            context -> {
              Rebalancer rebalancer = context.getBean(Rebalancer.class);
              Assert.assertNotNull(rebalancer);
              Assert.assertTrue(
                  rebalancer instanceof RebalancerAutoConfiguration2.SimpleRebalancer);
            });
  }
}
