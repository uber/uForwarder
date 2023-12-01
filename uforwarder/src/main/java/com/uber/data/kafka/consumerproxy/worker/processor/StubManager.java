package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.common.StructuredTags;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.m3.tally.Scope;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StubManager manages message stub. every message received has a associated stub created and
 * removed when ack received.
 */
class StubManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StubManager.class);
  private final ConcurrentMap<TopicPartition, StubMap> topicPartitionStubMap;
  private final Scope scope;

  /**
   * Instantiates a new Stub manager.
   *
   * @param scope the scope
   */
  StubManager(Scope scope) {
    this.topicPartitionStubMap = new ConcurrentHashMap<>();
    this.scope = scope;
  }

  /**
   * Cancels the topic partition
   *
   * @param topicPartition the topic partition
   */
  void cancel(TopicPartition topicPartition) {
    StubMap stubMap = topicPartitionStubMap.get(topicPartition);
    if (stubMap == null) {
      logAndReportUnassignedTopicPartition(topicPartition);
      // do not need to do anything
    } else {
      topicPartitionStubMap.remove(topicPartition);
    }
  }

  /** Cancels all topic partition */
  void cancelAll() {
    topicPartitionStubMap.clear();
  }

  /**
   * Subscribes the topic partition
   *
   * @param job the job
   */
  void init(Job job) {
    TopicPartition topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    topicPartitionStubMap.computeIfAbsent(topicPartition, tp -> new StubMap(job));
  }

  /**
   * Receives a processor message. remember the corresponding stub
   *
   * @param pm the pm
   */
  void receive(ProcessorMessage pm) {
    TopicPartitionOffset topicPartitionOffset = pm.getPhysicalMetadata();
    TopicPartition tp =
        new TopicPartition(
            pm.getPhysicalMetadata().getTopic(), pm.getPhysicalMetadata().getPartition());

    StubMap stubMap = topicPartitionStubMap.get(tp);
    if (stubMap == null) {
      logAndReportUnassignedTopicPartition(tp);
      throw new IllegalStateException(
          String.format("topic-partition %s has not been assigned to this message processor", tp));
    }

    stubMap.receive(topicPartitionOffset.getOffset(), pm.getStub());
  }

  /**
   * Removes a process message and corresponding stub
   *
   * @param pm the pm
   * @return boolean indicates if remove succeed
   */
  boolean ack(ProcessorMessage pm) {
    TopicPartitionOffset topicPartitionOffset = pm.getPhysicalMetadata();
    TopicPartition tp =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    StubMap stubMap = topicPartitionStubMap.get(tp);
    if (stubMap == null) {
      logAndReportUnassignedTopicPartition(tp);
      // this topic-partition is not tracked, so do nothing
    } else {
      MessageStub result = stubMap.remove(topicPartitionOffset.getOffset());
      if (result == null) {
        logAndReportInvalidOffset(topicPartitionOffset);
      } else {
        return true;
      }
    }

    return false;
  }

  /**
   * Marks a message nacked
   *
   * @param topicPartitionOffset
   */
  void nack(TopicPartitionOffset topicPartitionOffset) {
    // NOOP
  }

  /**
   * Gets Stub
   *
   * @param topicPartitionOffset
   * @return Optional.empty() if stub doesn't exist
   */
  Optional<MessageStub> getStub(TopicPartitionOffset topicPartitionOffset) {
    TopicPartition tp =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    StubMap stubMap = topicPartitionStubMap.get(tp);
    if (stubMap == null) {
      logAndReportUnassignedTopicPartition(tp);
      // this topic-partition is not tracked, so do nothing
      return Optional.empty();
    } else {
      return stubMap.get(topicPartitionOffset.getOffset());
    }
  }

  Map<Job, Map<Long, MessageStub>> getStubs() {
    return topicPartitionStubMap
        .values()
        .stream()
        .collect(Collectors.toMap(StubMap::getJob, StubMap::getStubs));
  }

  protected void logAndReportUnassignedTopicPartition(TopicPartition tp) {
    LOGGER.error(
        "a topic partition has not been assigned to this stub manager",
        StructuredLogging.kafkaTopic(tp.topic()),
        StructuredLogging.kafkaPartition(tp.partition()));
    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaTopic(tp.topic())
                .setKafkaPartition(tp.partition())
                .build())
        .counter(MetricNames.STUB_MANAGER_UNASSIGNED)
        .inc(1);
  }

  private void logAndReportInvalidOffset(TopicPartitionOffset topicPartitionOffset) {
    LOGGER.error(
        "a topic partition offset was not found in stub manager",
        StructuredLogging.kafkaTopic(topicPartitionOffset.getTopic()),
        StructuredLogging.kafkaPartition(topicPartitionOffset.getPartition()),
        StructuredLogging.kafkaOffset(topicPartitionOffset.getOffset()));
    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaTopic(topicPartitionOffset.getTopic())
                .setKafkaPartition(topicPartitionOffset.getPartition())
                .build())
        .counter(MetricNames.STUB_MANAGER_INVALID_OFFSET)
        .inc(1);
  }

  private class StubMap {
    private final Job job;
    /** mapping from message offset to stub */
    private Map<Long, MessageStub> stubs;

    private StubMap(Job job) {
      this.job = job;
      this.stubs = new ConcurrentHashMap<>();
    }

    private Job getJob() {
      return job;
    }

    private Map<Long, MessageStub> getStubs() {
      return stubs;
    }

    /**
     * Creates a stub. if there is existing stub, create a new one and replace it
     *
     * @param offset the offset
     * @return the stub
     */
    void receive(long offset, MessageStub stub) {
      stubs.put(offset, stub);
    }

    /**
     * Removes a stub.
     *
     * @param offset the offset
     * @return the stub
     */
    MessageStub remove(long offset) {
      return stubs.remove(offset);
    }

    Optional<MessageStub> get(long offset) {
      return Optional.ofNullable(stubs.get(offset));
    }
  }

  private static class MetricNames {
    static final String STUB_MANAGER_UNASSIGNED = "processor.stub-message-manager.unassigned";
    static final String STUB_MANAGER_INVALID_OFFSET =
        "processor.stub-message-manager.invalid-offset";
  }
}
