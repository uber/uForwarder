package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import com.uber.m3.tally.Scope;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AckManager limits the number of unprocessed messages, and tracks received/ack/nack status of each
 * message
 */
class AckManager implements MetricSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(AckManager.class);

  private final MessageAckStatusManager messageAckStatusManager;
  private final UnprocessedMessageManager unprocessedMessageManager;
  private final BlockingQueueStubManager stubManager;

  AckManager(
      MessageAckStatusManager messageAckStatusManager,
      UnprocessedMessageManager unprocessedMessageManager,
      Scope rootScope) {
    this(
        messageAckStatusManager,
        unprocessedMessageManager,
        new BlockingQueueStubManager(rootScope));
  }

  @VisibleForTesting
  AckManager(
      MessageAckStatusManager messageAckStatusManager,
      UnprocessedMessageManager unprocessedMessageManager,
      BlockingQueueStubManager stubManager) {
    this.messageAckStatusManager = messageAckStatusManager;
    this.unprocessedMessageManager = unprocessedMessageManager;
    this.stubManager = stubManager;
    stubManager.addBlockingQueue(unprocessedMessageManager);
    stubManager.addBlockingQueue(messageAckStatusManager);
  }

  /**
   * initializes the state for a job
   *
   * @param job a job
   */
  void init(Job job) {
    // do not change the order as the downstream needs to be initialized first
    stubManager.init(job);
    messageAckStatusManager.init(job);
    unprocessedMessageManager.init(job);
  }

  /**
   * cancels a job
   *
   * @param job a job
   */
  void cancel(Job job) {
    // do not change the order as the upstream needs to be canceled first
    TopicPartition topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    unprocessedMessageManager.cancel(topicPartition);
    messageAckStatusManager.cancel(job);
    stubManager.cancel(topicPartition);
  }

  /** cancels all jobs */
  void cancelAll() {
    // do not change the order as the upstream needs to be canceled first
    unprocessedMessageManager.cancelAll();
    messageAckStatusManager.cancelAll();
    stubManager.cancelAll();
  }

  /**
   * marks a message as received (and unprocessed). this method may be blocked if the number of
   * unprocessed message reaches a threshold. See {@code UnprocessedMessageManager#receive} for
   * details.
   *
   * @param processorMessage the received KafkaMessage
   */
  void receive(ProcessorMessage processorMessage) {
    // do not change the order as we need to limit the unprocessed message count first
    unprocessedMessageManager.receive(processorMessage);
    stubManager.receive(processorMessage);
    messageAckStatusManager.receive(processorMessage);
  }

  /**
   * Marks a message as acked, and determines whether an offset should be committed to kafka servers
   * or not: if the returned value is negative, the caller should not commit to kafka servers;
   * otherwise, the caller should commit the returned value to kafka servers.
   *
   * <p>Meanwhile, removes the message from unprocessed message cache.
   *
   * @param processorMessage the received KafkaMessage
   * @return the offset to commit to kafka servers if it's not negative; should not commit to kafka
   *     servers if it's negative.
   */
  long ack(ProcessorMessage processorMessage) {
    TopicPartitionOffset topicPartitionOffset = processorMessage.getPhysicalMetadata();
    // do not change the order as we need to limit the unprocessed message count
    long offsetToCommit = messageAckStatusManager.ack(topicPartitionOffset);
    unprocessedMessageManager.remove(processorMessage);
    stubManager.ack(processorMessage);
    return offsetToCommit;
  }

  /**
   * marks a message as nacked, and determines whether the caller should proceed to produce the
   * message to the retry topic or dlq topic: if true, then the caller should; if false, then the
   * caller can skip producing the message to the retry topic or dlq topic (but it does no harm if
   * the caller still produces the message to the retry topic or dlq topic), notice that it does NOT
   * indicate that something is wrong.
   *
   * @param topicPartitionOffset the physical metadata of the message to be nacked
   * @return true -- the caller should proceed to produce the message to the retry topic or dlq
   *     topic; false -- the caller can skip producing the message to the retry topic or dlq topic
   *     (but it does no harm if the caller still produces the message to the retry topic or dlq
   *     topic), notice that it does NOT indicate that something is wrong.
   */
  boolean nack(TopicPartitionOffset topicPartitionOffset) {
    boolean result = messageAckStatusManager.nack(topicPartitionOffset);
    stubManager.nack(topicPartitionOffset);
    return result;
  }

  Map<Job, Map<Long, MessageStub>> getStubs() {
    return stubManager.getStubs();
  }

  @Override
  public void publishMetrics() {
    unprocessedMessageManager.publishMetrics();
    messageAckStatusManager.publishMetrics();
  }
}
