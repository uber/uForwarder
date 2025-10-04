package com.uber.data.kafka.datatransfer.controller.rebalancer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

public class RebalancerAutoConfigurationTest {

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
              Assertions.assertNotNull(rebalancer);
              Assertions.assertFalse(
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
              Assertions.assertNotNull(rebalancer);
              Assertions.assertTrue(
                  rebalancer instanceof RebalancerAutoConfiguration2.SimpleRebalancer);
            });
  }
}
