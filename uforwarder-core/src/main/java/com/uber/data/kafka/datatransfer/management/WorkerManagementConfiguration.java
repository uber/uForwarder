package com.uber.data.kafka.datatransfer.management;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "management.worker")
public class WorkerManagementConfiguration {
  @Value("${uber.region}")
  private String uberRegion = "";

  private String workerUdg = "";

  public String getWorkerUdg() {
    return workerUdg;
  }

  public void setWorkerUdg(String workerUdg) {
    this.workerUdg = workerUdg;
  }

  public String getUberRegion() {
    return uberRegion;
  }
}
