package com.uber.data.kafka.consumerproxy;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.datatransfer.controller.DataTransferMaster;
import com.uber.data.kafka.datatransfer.worker.DataTransferWorker;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.StandardEnvironment;

@SpringBootApplication
@SuppressWarnings("PrivateConstructorForUtilityClass")
public class UForwarder {

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(UForwarder.class);
    String[] profiles = provideActiveProfile(args);
    app.setEnvironment(buildConfigurationEnvironment(profiles));
    System.setProperty("spring.profiles.active", String.join(",", profiles));
    app.setAdditionalProfiles(profiles);

    app.run(UForwarder.class, args);
  }

  @VisibleForTesting
  static ConfigurableEnvironment buildConfigurationEnvironment(String[] profiles) {
    ConfigurableEnvironment configurableEnvironment = new StandardEnvironment();
    configurableEnvironment.setActiveProfiles(profiles);
    return configurableEnvironment;
  }

  @VisibleForTesting
  static String[] provideActiveProfile(String[] args) {
    String appType = null;
    // try to get app type from parameters first.
    if (args != null && args.length > 0) {
      appType = args[0];
    }
    if (appType == null) {
      appType = "";
    }
    return getActiveProfiles(appType);
  }

  @VisibleForTesting
  static String[] getActiveProfiles(String appType) {
    switch (appType) {
      case UForwarderAppType.CONTROLLER_APP:
        return new String[] {DataTransferMaster.SPRING_PROFILE, UForwarderAppType.CONTROLLER_APP};
      case UForwarderAppType.WORKER_APP:
        return new String[] {DataTransferWorker.SPRING_PROFILE, UForwarderAppType.WORKER_APP};
      default:
        return new String[] {};
    }
  }
}
