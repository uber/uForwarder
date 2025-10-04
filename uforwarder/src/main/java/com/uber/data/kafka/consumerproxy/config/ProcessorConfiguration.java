package com.uber.data.kafka.consumerproxy.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("worker.processor")
public class ProcessorConfiguration {

  // This is the size of the ExecutorService that executes async operations.
  // This value is specific to a pipeline.
  // The total number of threads for processors = pipelineCount * threadPoolSize.
  private int threadPoolSize = 1;

  // This is the default unprocessed inbounded message count for each topic partition.
  private int maxInboundCacheCount = 1000;

  // This is the default unprocessed inbound message count for the processor
  // actual unprocessed inbound message count limit equals to min(maxProcessorInBoundCacheCount,
  // nPartitions * maxInboundCacheCount)
  private int maxProcessorInBoundCacheCount = 10000;

  // This is the default unprocessed inbound message byte size for each topic partition
  private int maxInboundCacheByteSize = 10 * 1024 * 1024;

  // This is shared unprocessed inbound message cache size, please make sure it's larger than
  // max message bytes size the system support
  // Message processor acquire a permit from local limiter, if failed, then from shared limiter
  private int sharedInboundCacheByteSize = 512 * 1024 * 1024;

  // This is the default unprocessed outbound message count for each topic partition if there is no
  // valid configuration provided by the master in the JobConfiguration, and max unprocessed
  // outbound
  // message count of each topic partition for adaptive limiter
  private int maxOutboundCacheCount = 250;

  // This is the default max difference between an ACKed offset and the largest committed offset for
  // each topic partition.
  private int maxAckCommitSkew = 10000;

  // The cluster filter, if enabled, filters out (does not send to callee) Kafka messages where
  // the record header "cluster" field does not match the source cluster name when not empty.
  private boolean clusterFilterEnabled = false;

  // use experimental limiter
  private boolean experimentalLimiterEnabled = false;

  public int getThreadPoolSize() {
    return threadPoolSize;
  }

  public void setThreadPoolSize(int threadPoolSize) {
    this.threadPoolSize = threadPoolSize;
  }

  public int getMaxInboundCacheCount() {
    return maxInboundCacheCount;
  }

  public void setMaxInboundCacheCount(int maxInboundCacheCount) {
    this.maxInboundCacheCount = maxInboundCacheCount;
  }

  public int getMaxInboundCacheByteSize() {
    return maxInboundCacheByteSize;
  }

  public void setMaxInboundCacheByteSize(int maxInboundCacheByteSize) {
    this.maxInboundCacheByteSize = maxInboundCacheByteSize;
  }

  public int getSharedInboundCacheByteSize() {
    return sharedInboundCacheByteSize;
  }

  public void setSharedInboundCacheByteSize(int sharedInboundCacheByteSize) {
    this.sharedInboundCacheByteSize = sharedInboundCacheByteSize;
  }

  public int getMaxOutboundCacheCount() {
    return maxOutboundCacheCount;
  }

  public void setMaxOutboundCacheCount(int maxOutboundCacheCount) {
    this.maxOutboundCacheCount = maxOutboundCacheCount;
  }

  public boolean isExperimentalLimiterEnabled() {
    return experimentalLimiterEnabled;
  }

  public void setExperimentalLimiterEnabled(boolean experimentalLimiterEnabled) {
    this.experimentalLimiterEnabled = experimentalLimiterEnabled;
  }

  public int getMaxAckCommitSkew() {
    return maxAckCommitSkew;
  }

  public void setMaxAckCommitSkew(int maxAckCommitSkew) {
    this.maxAckCommitSkew = maxAckCommitSkew;
  }

  public boolean isClusterFilterEnabled() {
    return clusterFilterEnabled;
  }

  public void setClusterFilterEnabled(boolean enableClusterFilter) {
    this.clusterFilterEnabled = enableClusterFilter;
  }

  public int getMaxProcessorInBoundCacheCount() {
    return maxProcessorInBoundCacheCount;
  }

  public void setMaxProcessorInBoundCacheCount(int maxProcessorInBoundCacheCount) {
    this.maxProcessorInBoundCacheCount = maxProcessorInBoundCacheCount;
  }
}
