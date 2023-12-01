package com.uber.data.kafka.consumerproxy.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("worker.dispatcher.grpc")
public class GrpcDispatcherConfiguration {
  private long minRpcTimeoutMs = 1;
  private long maxRpcTimeoutMs = 1800000;
  private int grpcChannelPoolSize = 1;

  public long getMinRpcTimeoutMs() {
    return minRpcTimeoutMs;
  }

  public void setMinRpcTimeoutMs(long minRpcTimeoutMs) {
    this.minRpcTimeoutMs = minRpcTimeoutMs;
  }

  public long getMaxRpcTimeoutMs() {
    return maxRpcTimeoutMs;
  }

  public void setMaxRpcTimeoutMs(long maxRpcTimeoutMs) {
    this.maxRpcTimeoutMs = maxRpcTimeoutMs;
  }

  public int getGrpcChannelPoolSize() {
    return grpcChannelPoolSize;
  }

  public void setGrpcChannelPoolSize(int grpcChannelPoolSize) {
    this.grpcChannelPoolSize = grpcChannelPoolSize;
  }
}
