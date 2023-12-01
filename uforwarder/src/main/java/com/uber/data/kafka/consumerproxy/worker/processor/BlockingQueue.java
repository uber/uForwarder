package com.uber.data.kafka.consumerproxy.worker.processor;

import java.util.Optional;
import org.apache.kafka.common.TopicPartition;

/**
 * Represent a queue that support both blocking message detection and cancellation
 *
 * <pre>
 * blocking message detection - blocking message normally is the message on the head of the queue
 * blocking message cancellation - marks message as canceled, a canceled message should not be detected as a blocking message
 * </pre>
 */
public interface BlockingQueue {

  /**
   * Detects the blocking message A blocking message is usually on the head of queue
   *
   * @param topicPartition the topic partition
   * @return the blocking message
   */
  Optional<BlockingMessage> detectBlockingMessage(TopicPartition topicPartition);

  /**
   * Marks a message as canceled. A canceled message should not be treated as blocking message
   *
   * @param topicPartitionOffset the topic partition offset
   * @return the boolean indicates if mark succeed
   */
  boolean markCanceled(TopicPartitionOffset topicPartitionOffset);

  /** The type Blocking message. */
  class BlockingMessage {
    private final TopicPartitionOffset metadata;
    private final BlockingReason reason;

    /**
     * Instantiates a new Blocking message.
     *
     * @param metadata the topic partition offset
     * @param reason the reason
     */
    public BlockingMessage(TopicPartitionOffset metadata, BlockingReason reason) {
      this.metadata = metadata;
      this.reason = reason;
    }

    /**
     * Gets blocking message metadata.
     *
     * @return the message metadata
     */
    public TopicPartitionOffset getMetaData() {
      return metadata;
    }

    /**
     * Gets blocking reason.
     *
     * @return the blocking reason
     */
    public BlockingReason getReason() {
      return reason;
    }
  }

  /** The message blocking reason. */
  enum BlockingReason {
    /** head of line blocked */
    BLOCKING,
    /** queue is saturated */
    SATURATION
  }
}
