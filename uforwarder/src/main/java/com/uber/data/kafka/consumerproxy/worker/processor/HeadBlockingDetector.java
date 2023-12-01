package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.m3.tally.Scope;
import java.util.Optional;

/**
 * {@link AckTrackingQueue} when it is full, the pipeline get blocked. there are 2 basic assumptions
 * that decided blocking can *only* be caused by downstream of the queue instead of upstream. 1.
 * There is {@link UnprocessedMessageManager} to limit number of unprocessed messages before enter
 * AckTrackingQueue 2. Capacity of a {@link AckTrackingQueue} is much higher than {@link
 * UnprocessedMessageManager}
 *
 * <p>There are 2 major causes could get {@link AckTrackingQueue} filled up or even block 1.
 * Availability missing of downstream, and messages fail randomly 2. Poison pill message kept
 * failing constantly
 *
 * <p>load factor of AckTrackingQueue is defined as size / capacity. when load factor above
 * threshold, head of line blocking detected
 */
public class HeadBlockingDetector {
  /** critical threshold of load factor */
  protected final double critical;
  /** minimal acked percentage to detect blocking */
  protected final double minAckPercent;

  /**
   * Instantiates a new Head blocking detector.
   *
   * @param builder the builder
   */
  protected HeadBlockingDetector(Builder builder) {
    this.critical = builder.critical;
    this.minAckPercent = builder.minAckPercent;
  }

  /**
   * Detects head of line blocking status on given AckTrackingQueue
   *
   * @return the result
   */
  public Optional<BlockingQueue.BlockingMessage> detect(
      Scope jobScope, AckTrackingQueue ackTrackingQueue) {
    BlockingQueue.BlockingReason reason = null;
    AckTrackingQueue.State state = ackTrackingQueue.getState();
    if (state.lowestCancelableOffset() == AbstractAckTrackingQueue.INITIAL_OFFSET) {
      // no cancelable message
      return Optional.empty();
    }

    if (isCritical(state) && hasMajorityAcked(state.stats())) {
      reason = BlockingQueue.BlockingReason.BLOCKING;
      jobScope.counter(MetricNames.BLOCKED_MESSAGES).inc(1);
    }
    if (reason == null) {
      return Optional.empty();
    } else {
      return Optional.of(
          new BlockingQueue.BlockingMessage(
              new TopicPartitionOffset(
                  ackTrackingQueue.getTopicPartition().topic(),
                  ackTrackingQueue.getTopicPartition().partition(),
                  state.lowestCancelableOffset()),
              reason));
    }
  }

  /**
   * Creates builder
   *
   * @return a builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Determines if the queue is about to be full
   *
   * @param state the state
   * @return true if the queue is about to be full
   */
  protected boolean isCritical(AckTrackingQueue.State state) {
    return state.loadFactor(true) > critical;
  }

  /**
   * Determines if majority of offsets have been acked.
   *
   * @param stats
   * @return true if majority is already acked
   */
  protected boolean hasMajorityAcked(AckTrackingQueue.Stats stats) {
    return ackPercent(stats) > minAckPercent;
  }

  /** percentage of messages has been acked */
  protected double ackPercent(AckTrackingQueue.Stats stats) {
    int size = stats.size();
    if (size == 0) {
      return 0.0d;
    }

    return stats.acked() / (double) size;
  }

  /** The type Builder. */
  public static class Builder {
    private double critical = 0.9;
    private double minAckPercent = 0.98;

    private Builder() {}

    /**
     * Sets critical threshold of load factor.
     *
     * @param critical the critical
     * @return the critical
     */
    public Builder setCritical(double critical) {
      this.critical = critical;
      return this;
    }

    /**
     * Sets minimal acked percentage to detect blocking
     *
     * @param minAckPercent minimal acked percentage to detect blocking
     * @return the limit
     */
    public Builder setMinAckPercent(double minAckPercent) {
      this.minAckPercent = minAckPercent;
      return this;
    }

    /**
     * Builds head blocking detector.
     *
     * @return the head blocking detector
     */
    public HeadBlockingDetector build() {
      return new HeadBlockingDetector(this);
    }
  }

  class MetricNames {
    static final String BLOCKED_MESSAGES = "tracking-queue.blocking.messages";
  }
}
