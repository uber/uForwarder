package com.uber.data.kafka.datatransfer.controller.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "master.zookeeper")
public class ZookeeperConfiguration {
  private String zkConnection = "localhost:2181/kafka-consumer-proxy";

  public String getZkConnection() {
    return zkConnection;
  }

  public void setZkConnection(String zkConnection) {
    this.zkConnection = zkConnection;
  }
}
