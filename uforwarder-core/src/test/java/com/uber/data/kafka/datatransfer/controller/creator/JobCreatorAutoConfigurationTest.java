package com.uber.data.kafka.datatransfer.controller.creator;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

public class JobCreatorAutoConfigurationTest extends FievelTestBase {

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
              Assert.assertNotNull(jobCreator);
              Assert.assertFalse(
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
              Assert.assertNotNull(jobCreator);
              Assert.assertTrue(
                  jobCreator instanceof JobCreatorAutoConfiguration2.SimpleJobCreator);
            });
  }
}
