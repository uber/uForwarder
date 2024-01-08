package com.uber.data.kafka.datatransfer.controller.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "master.zookeeper")
public class ZookeeperConfiguration {
  private String zkConnection = "localhost:2181/kafka-consumer-proxy";
  // uforwarder controller can't start without zookeeper root node. the feature enables auto
  // creation of root node
  // however, it could leads to gray failure if zk root path was modified by accident
  // so it's ideal to enable for test environment only
  private boolean autoCreateRootNode = false;

  public String getZkConnection() {
    return zkConnection;
  }

  public void setZkConnection(String zkConnection) {
    this.zkConnection = zkConnection;
  }

  public boolean isAutoCreateRootNode() {
    return autoCreateRootNode;
  }

  public void setAutoCreateRootNode(boolean autoCreateRootNode) {
    this.autoCreateRootNode = autoCreateRootNode;
  }
}
