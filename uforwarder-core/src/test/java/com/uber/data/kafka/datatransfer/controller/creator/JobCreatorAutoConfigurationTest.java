package com.uber.data.kafka.datatransfer.controller.creator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

public class JobCreatorAutoConfigurationTest {

  @Test
  public void testInitialized() {
    new ApplicationContextRunner()
        .withAllowBeanDefinitionOverriding(true)
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withConfiguration(AutoConfigurations.of(JobCreatorAutoConfiguration.class))
        .withSystemProperties("spring.config.location=classpath:/base.yaml")
        .withPropertyValues("spring.profiles.active=data-transfer-controller")
        .run(
            context -> {
              JobCreator jobCreator = context.getBean(JobCreator.class);
              Assertions.assertNotNull(jobCreator);
              Assertions.assertFalse(
                  jobCreator instanceof JobCreatorAutoConfiguration2.SimpleJobCreator);
            });
  }

  @Test
  public void testInitializedNoConflict() {
    new ApplicationContextRunner()
        .withAllowBeanDefinitionOverriding(true)
        .withInitializer(new ConfigDataApplicationContextInitializer())
        .withConfiguration(
            AutoConfigurations.of(
                JobCreatorAutoConfiguration.class, JobCreatorAutoConfiguration2.class))
        .withSystemProperties("spring.config.location=classpath:/base.yaml")
        .withPropertyValues("spring.profiles.active=data-transfer-controller")
        .run(
            context -> {
              JobCreator jobCreator = context.getBean(JobCreator.class);
              Assertions.assertNotNull(jobCreator);
              Assertions.assertTrue(
                  jobCreator instanceof JobCreatorAutoConfiguration2.SimpleJobCreator);
            });
  }
}
