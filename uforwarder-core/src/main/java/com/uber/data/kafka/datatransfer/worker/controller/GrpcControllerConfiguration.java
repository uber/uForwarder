package com.uber.data.kafka.datatransfer.worker.controller;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "worker.controller.grpc")
public class GrpcControllerConfiguration {
  private String masterHostPort = "localhost:8000";
  private String masterUdgPath = "";
  private Duration heartbeatInterval = Duration.ofSeconds(3);
  private Duration heartbeatTimeout = Duration.ofSeconds(10);
  private Duration workerLease = Duration.ofMinutes(2);
  private int commandExecutorPoolSize = 1;

  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  public void setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
  }

  public Duration getHeartbeatTimeout() {
    return heartbeatTimeout;
  }

  public void setHeartbeatTimeout(Duration heartbeatTimeout) {
    this.heartbeatTimeout = heartbeatTimeout;
  }

  public Duration getWorkerLease() {
    return workerLease;
  }

  public void setWorkerLease(Duration lease) {
    this.workerLease = lease;
  }

  public String getMasterHostPort() {
    return masterHostPort;
  }

  public void setMasterHostPort(String masterHostPort) {
    this.masterHostPort = masterHostPort;
  }

  // TODO: refactor configuration naming
  public String getMasterUdgPath() {
    return masterUdgPath;
  }

  public void setMasterUdgPath(String masterUdgPath) {
    this.masterUdgPath = masterUdgPath;
  }

  public int getCommandExecutorPoolSize() {
    return commandExecutorPoolSize;
  }

  public void setCommandExecutorPoolSize(int commandExecutorPoolSize) {
    this.commandExecutorPoolSize = commandExecutorPoolSize;
  }
}
