package com.uber.data.kafka.consumerproxy.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("worker.dispatcher.grpc")
public class GrpcDispatcherConfiguration {
  private long minRpcTimeoutMs = 1;
  private long maxRpcTimeoutMs = 1800000;
  private int grpcChannelPoolSize = 1;
  private int maxConcurrentStreams = 250;
  private int threadPoolSize = 0;

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

  /**
   * Gets MAX_CONCURRENT_STREAMS limit of HTTP/2
   *
   * @return the max concurrent streams
   */
  public int getMaxConcurrentStreams() {
    return maxConcurrentStreams;
  }

  /**
   * Sets MAX_CONCURRENT_STREAMS limit of HTTP/2
   *
   * @param maxConcurrentStreams the max concurrent streams
   */
  public void setMaxConcurrentStreams(int maxConcurrentStreams) {
    this.maxConcurrentStreams = maxConcurrentStreams;
  }

  /**
   * Gets thread pool size, 0 to use grpc default thread pool
   *
   * @return the thread pool size
   */
  public int getThreadPoolSize() {
    return threadPoolSize;
  }

  /**
   * Sets thread pool size, set to 0 to use grpc default thread pool
   *
   * @param threadPoolSize
   */
  public void setThreadPoolSize(int threadPoolSize) {
    this.threadPoolSize = threadPoolSize;
  }
}
