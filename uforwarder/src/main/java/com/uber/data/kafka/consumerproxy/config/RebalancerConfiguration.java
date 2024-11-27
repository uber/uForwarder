package com.uber.data.kafka.consumerproxy.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("master.rebalancer")
public class RebalancerConfiguration {
  public static final double PLACEMENT_WORKER_SCALE_SOFT_LIMIT = 1.0;
  private static final Logger logger = LoggerFactory.getLogger(RebalancerConfiguration.class);
  private static final long DEFAULT_MESSAGES_PER_SEC_PER_WORKER = Long.MAX_VALUE;

  // This sets a static number of workers to use per RPC uri.
  private int numWorkersPerUri = 2;

  // This sets the rebalancer mode: StreamingRpcUriRebalancer | BatchRpcUriRebalancer.
  private String mode = "default";

  // messagesPerSecPerWorker is the capacity per worker.
  // default to MAX Long, which means it will use numWorkersPerUri to allocate worker
  private long messagesPerSecPerWorker = DEFAULT_MESSAGES_PER_SEC_PER_WORKER;

  // This sets the target spare worker percentage per cluster
  // Per https://engwiki.uberinternal.com/display/UPCLOUD/Understanding+Rollout+Policies service
  // must be able to handle 12.5% of the instances being down during upgrade of a zone.
  // https://stack.uberinternal.com/questions/61339
  private int targetSpareWorkerPercentage = 13;

  // This sets the minimum worker count service should have
  private int minimumWorkerCount = 5;

  // Max number of hibernating JobGroups Per Worker
  private int hibernatingJobGroupPerWorker = 15;

  // Max number of jobs per worker
  private int maxJobNumberPerWorker = 500;

  // Maximum limit for the consistent-hashing rebalancer hash value range
  private long maxAssignmentHashValueRange = 10000;

  // Number of virtual partitions for job placement
  private int numberOfVirtualPartitions = 4;

  // Whether to enable shadow rebalancer
  private boolean shouldRunShadowRebalancer = false;

  // The ratio of worker to remove in a batch when workload reduces
  private double workerToReduceRatio = 0.1;

  // The system should use the estimated total worker based on soft limit(1.0) to scale cluster
  // size,
  // but rebalancer can use hard limit to pack workload onto workers. The rebalance can keep
  // placement optimized within the graceful area between soft and hard limit.
  private double placementWorkerScaleHardLimit = 1.0;

  public int getNumWorkersPerUri() {
    return numWorkersPerUri;
  }

  public void setNumWorkersPerUri(int numWorkersPerUri) {
    this.numWorkersPerUri = numWorkersPerUri;
  }

  /**
   * Gets the rebalancer mode: StreamingRpcUriRebalancer | BatchRpcUriRebalancer
   *
   * @return the rebalancer mode
   */
  public String getMode() {
    return mode;
  }

  /**
   * Sets the rebalancer mode: StreamingRpcUriRebalancer | BatchRpcUriRebalancer
   *
   * @param mode the rebalancer mode
   */
  public void setMode(String mode) {
    this.mode = mode;
  }

  /**
   * Gets the number of workers to use per RPC uri
   *
   * @return the number of workers to use
   */
  public long getMessagesPerSecPerWorker() {
    return messagesPerSecPerWorker;
  }

  /**
   * Sets the number of workers to use per RPC uri
   *
   * @param messagesPerSecPerWorker the number of workers to use
   */
  public void setMessagesPerSecPerWorker(long messagesPerSecPerWorker) {
    if (messagesPerSecPerWorker <= 0) {
      throw new IllegalArgumentException("messagesPerSecPerWorker must > 0");
    }
    this.messagesPerSecPerWorker = messagesPerSecPerWorker;
  }

  /**
   * Gets the target spare worker percentage
   *
   * @return the target spare worker percentage
   */
  public int getTargetSpareWorkerPercentage() {
    return targetSpareWorkerPercentage;
  }

  /**
   * Sets the target spare worker percentage
   *
   * @param targetSpareWorkerPercentage the target spare worker percentage
   */
  public void setTargetSpareWorkerPercentage(int targetSpareWorkerPercentage) {
    this.targetSpareWorkerPercentage = targetSpareWorkerPercentage;
  }

  /**
   * Gets the minimum worker count for the service
   *
   * @return the minimum worker count for the service
   */
  public int getMinimumWorkerCount() {
    return minimumWorkerCount;
  }

  /**
   * Sets the minimum worker count for the service
   *
   * @param minimumWorkerCount minimum worker count for the service
   */
  public void setMinimumWorkerCount(int minimumWorkerCount) {
    this.minimumWorkerCount = minimumWorkerCount;
  }

  /**
   * Gets hibernating job group per worker.
   *
   * @return the hibernating job group per worker
   */
  public int getHibernatingJobGroupPerWorker() {
    return hibernatingJobGroupPerWorker;
  }

  /**
   * Sets hibernating job group per worker.
   *
   * @param hibernatingJobGroupPerWorker the hibernating job group per worker
   */
  public void setHibernatingJobGroupPerWorker(int hibernatingJobGroupPerWorker) {
    this.hibernatingJobGroupPerWorker = hibernatingJobGroupPerWorker;
  }

  /**
   * Gets max job number per worker
   *
   * @return The max job number
   */
  public int getMaxJobNumberPerWorker() {
    return maxJobNumberPerWorker;
  }

  /** Sets the max job number per worker */
  public void setMaxJobNumberPerWorker(int maxJobNumberPerWorker) {
    this.maxJobNumberPerWorker = maxJobNumberPerWorker;
  }

  /**
   * Gets max assignment hash value range
   *
   * @return The max range value
   */
  public long getMaxAssignmentHashValueRange() {
    return maxAssignmentHashValueRange;
  }

  /**
   * Sets max assignment hash value range
   *
   * @param maxAssignmentHashValueRange The expected range
   */
  public void setMaxAssignmentHashValueRange(long maxAssignmentHashValueRange) {
    this.maxAssignmentHashValueRange = maxAssignmentHashValueRange;
  }

  /**
   * Gets number of virtual partitions for job placement
   *
   * @return The number of virutal partitions
   */
  public int getNumberOfVirtualPartitions() {
    return numberOfVirtualPartitions;
  }

  /**
   * Sets number of virtual partitions for job placement
   *
   * @param numberOfVirtualPartitions The expected number of virtual partitions
   */
  public void setNumberOfVirtualPartitions(int numberOfVirtualPartitions) {
    this.numberOfVirtualPartitions = numberOfVirtualPartitions;
  }

  /** Gets the flag on whether to run shadow rebalancer */
  public boolean getShouldRunShadowRebalancer() {
    return shouldRunShadowRebalancer;
  }

  /**
   * Sets the flag to whether to run shadow rebalancer
   *
   * @param shouldRunShadowRebalancer The intended shadow mode
   */
  public void setShouldRunShadowRebalancer(boolean shouldRunShadowRebalancer) {
    this.shouldRunShadowRebalancer = shouldRunShadowRebalancer;
  }

  /**
   * Sets the worker to reduce ratio
   *
   * @param workerToReduceRatio The intended number of worker
   */
  public void setWorkerToReduceRatio(double workerToReduceRatio) {
    this.workerToReduceRatio = workerToReduceRatio;
  }

  /** Gets the worker to reduce ratio */
  public double getWorkerToReduceRatio() {
    return this.workerToReduceRatio;
  }

  /** Sets the worker scale hard limit for job placement */
  public void setPlacementWorkerScaleHardLimit(double placementWorkerScaleHardLimit) {
    if (placementWorkerScaleHardLimit < PLACEMENT_WORKER_SCALE_SOFT_LIMIT) {
      logger.error(
          "placementWorkerScaleHardLimit should not be smaller than 1.0, setting it to 1.0");
      placementWorkerScaleHardLimit = PLACEMENT_WORKER_SCALE_SOFT_LIMIT;
    }
    this.placementWorkerScaleHardLimit = placementWorkerScaleHardLimit;
  }

  /* Gets the worker scale hard limit for job placement */
  public double getPlacementWorkerScaleHardLimit() {
    return this.placementWorkerScaleHardLimit;
  }
}
