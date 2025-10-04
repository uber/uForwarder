package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.consumerproxy.common.MetricsUtils;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.common.StructuredTags;
import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import com.uber.m3.tally.Scope;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MessageAckStatusManager tracks ack status of each message for each topic partition */
public class MessageAckStatusManager implements MetricSource, BlockingQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageAckStatusManager.class);
  private static final double DEFAULT_MIN_ACK_PERCENT = 0.98;
  protected final ConcurrentMap<TopicPartition, QueueAndScope> tpAckMap;
  protected final Scope scope;
  private final int ackTrackingQueueSize;
  private final boolean useLinkedListAckTrackingQueue;
  private final HeadBlockingDetector headBlockingDetector;

  MessageAckStatusManager(
      int ackTrackingQueueSize, boolean useLinkedListAckTrackingQueue, Scope scope) {
    this(
        ackTrackingQueueSize,
        useLinkedListAckTrackingQueue,
        HeadBlockingDetector.newBuilder(),
        scope);
  }

  @VisibleForTesting
  MessageAckStatusManager(
      int ackTrackingQueueSize,
      boolean useLinkedListAckTrackingQueue,
      HeadBlockingDetector.Builder builder,
      Scope scope) {
    this.ackTrackingQueueSize = ackTrackingQueueSize;
    this.useLinkedListAckTrackingQueue = useLinkedListAckTrackingQueue;
    this.tpAckMap = new ConcurrentHashMap<>();
    this.scope = scope;
    this.headBlockingDetector = builder.build();
  }

  @Override
  public Optional<BlockingMessage> detectBlockingMessage(TopicPartition topicPartition) {
    QueueAndScope item = tpAckMap.get(topicPartition);
    if (item == null) {
      logAndReportUnassignedTopicPartition(topicPartition);
      return Optional.empty();
    }

    return headBlockingDetector.detect(item.jobScope, item.ackTrackingQueue);
  }

  @Override
  public boolean markCanceled(TopicPartitionOffset topicPartitionOffset) {
    TopicPartition topicPartition =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    QueueAndScope item = tpAckMap.get(topicPartition);
    if (item == null) {
      logAndReportUnassignedTopicPartition(topicPartition);
      return false;
    }
    try {
      return item.ackTrackingQueue.cancel(topicPartitionOffset.getOffset() + 1);
    } catch (InterruptedException e) {
      logAndReportInterrupted(topicPartitionOffset, e);
      return false;
    }
  }

  @Override
  public void publishMetrics() {
    for (QueueAndScope queueAndScope : tpAckMap.values()) {
      queueAndScope.ackTrackingQueue.publishMetrics();
    }
  }

  void init(Job job) {
    // TODO (T4576653): determine if multiple creation creation by computeIfAbsent is ok.
    TopicPartition tp =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    tpAckMap.computeIfAbsent(tp, topicPartition -> new QueueAndScope(job));
  }

  void cancel(Job job) {
    TopicPartition tp =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    QueueAndScope item = tpAckMap.get(tp);
    if (item == null) {
      logAndReportUnassignedJob(job);
      // do not need to do anything
    } else {
      // unblocks all threads
      item.ackTrackingQueue.markAsNotInUse();
      tpAckMap.remove(tp);
    }
  }

  void cancelAll() {
    for (QueueAndScope item : tpAckMap.values()) {
      item.ackTrackingQueue.markAsNotInUse();
    }
    tpAckMap.clear();
  }

  void receive(ProcessorMessage processorMessage) {
    TopicPartitionOffset topicPartitionOffset = processorMessage.getPhysicalMetadata();
    TopicPartition tp =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    QueueAndScope item = tpAckMap.get(tp);
    if (item == null) {
      logAndReportUnassignedTopicPartition(tp);
      throw new IllegalStateException(
          String.format("topic-partition %s has not been assigned to this message processor", tp));
    } else {
      try {
        doReceive(item.ackTrackingQueue, processorMessage);
      } catch (InterruptedException e) {
        logAndReportInterrupted(topicPartitionOffset, e);
      }
    }
  }

  protected void doReceive(AckTrackingQueue ackTrackingQueue, ProcessorMessage processorMessage)
      throws InterruptedException {
    ackTrackingQueue.receive(processorMessage.getPhysicalMetadata().getOffset());
  }

  long ack(TopicPartitionOffset topicPartitionOffset) {
    TopicPartition tp =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    QueueAndScope item = tpAckMap.get(tp);
    if (item == null) {
      logAndReportUnassignedTopicPartition(tp);
      // this topic-partition is not tracked, so we cannot ack
      return AckTrackingQueue.CANNOT_ACK;
    } else {
      try {
        return item.ackTrackingQueue.ack(topicPartitionOffset.getOffset() + 1);
      } catch (InterruptedException e) {
        logAndReportInterrupted(topicPartitionOffset, e);
        return AckTrackingQueue.CANNOT_ACK;
      }
    }
  }

  boolean nack(TopicPartitionOffset topicPartitionOffset) {
    TopicPartition tp =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    QueueAndScope item = tpAckMap.get(tp);
    if (item == null) {
      logAndReportUnassignedTopicPartition(tp);
      // this topic-partition is not tracked, so we cannot nack
      return false;
    } else {
      try {
        return item.ackTrackingQueue.nack(topicPartitionOffset.getOffset() + 1);
      } catch (InterruptedException e) {
        logAndReportInterrupted(topicPartitionOffset, e);
        return false;
      }
    }
  }

  protected void logAndReportInterrupted(
      TopicPartitionOffset topicPartitionOffset, InterruptedException e) {
    LOGGER.error(
        "the thread is interrupted while waiting on AckTrackingQueue operation",
        StructuredLogging.kafkaTopic(topicPartitionOffset.getTopic()),
        StructuredLogging.kafkaPartition(topicPartitionOffset.getPartition()),
        StructuredLogging.kafkaOffset(topicPartitionOffset.getOffset()),
        e);
    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaTopic(topicPartitionOffset.getTopic())
                .setKafkaPartition(topicPartitionOffset.getPartition())
                .build())
        .counter(MetricNames.MESSAGE_ACK_STATUS_MANAGER_MANAGER_INTERRUPTED)
        .inc(1);
  }

  private void logAndReportUnassignedJob(Job job) {
    LOGGER.error(
        "a topic partition has not been assigned to this message processor",
        StructuredLogging.jobId(job.getJobId()),
        StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
        StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
        StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
        StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()));
    scope
        .tagged(
            StructuredTags.builder()
                .setDestination(job.getRpcDispatcherTask().getUri())
                .setKafkaGroup(job.getKafkaConsumerTask().getConsumerGroup())
                .setKafkaTopic(job.getKafkaConsumerTask().getTopic())
                .setKafkaPartition(job.getKafkaConsumerTask().getPartition())
                .build())
        .counter(MetricNames.MESSAGE_ACK_STATUS_MANAGER_UNASSIGNED)
        .inc(1);
  }

  private void logAndReportUnassignedTopicPartition(TopicPartition tp) {
    LOGGER.error(
        "a topic partition has not been assigned to this message processor",
        StructuredLogging.kafkaTopic(tp.topic()),
        StructuredLogging.kafkaPartition(tp.partition()));
    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaTopic(tp.topic())
                .setKafkaPartition(tp.partition())
                .build())
        .counter(MetricNames.MESSAGE_ACK_STATUS_MANAGER_UNASSIGNED)
        .inc(1);
  }

  protected class QueueAndScope {
    protected final AckTrackingQueue ackTrackingQueue;
    protected final Scope jobScope;

    QueueAndScope(Job job) {
      this.jobScope = MetricsUtils.jobScope(scope, job);
      // create LinkedAckTrackingQueue when isolation level is read_committed to prevent data
      // loss caused by offset gaps
      // TODO: replace ArrayAckTrackingQueue with LinkedAckTrackingQueue to reduce maintenance
      // overhead
      this.ackTrackingQueue =
          job.getKafkaConsumerTask().getIsolationLevel()
                      == IsolationLevel.ISOLATION_LEVEL_READ_COMMITTED
                  || useLinkedListAckTrackingQueue
              ? new LinkedAckTrackingQueue(job, ackTrackingQueueSize, jobScope)
              : new ArrayAckTrackingQueue(job, ackTrackingQueueSize, jobScope);
    }
  }

  public static class Builder {
    protected final int ackTrackingQueueSize;
    protected final boolean useLinkedListAckTrackingQueue;
    protected final CoreInfra infra;

    public Builder(
        int ackTrackingQueueSize, boolean useLinkedListAckTrackingQueue, CoreInfra infra) {
      this.ackTrackingQueueSize = ackTrackingQueueSize;
      this.useLinkedListAckTrackingQueue = useLinkedListAckTrackingQueue;
      this.infra = infra;
    }

    MessageAckStatusManager build(Job job) {
      return new MessageAckStatusManager(
          ackTrackingQueueSize, useLinkedListAckTrackingQueue, infra.scope());
    }
  }

  private static class MetricNames {
    static final String MESSAGE_ACK_STATUS_MANAGER_UNASSIGNED =
        "processor.message-ack-status-manager.unassigned";
    static final String MESSAGE_ACK_STATUS_MANAGER_MANAGER_INTERRUPTED =
        "processor.message-ack-status-manager-manager.interrupted";
  }
}
