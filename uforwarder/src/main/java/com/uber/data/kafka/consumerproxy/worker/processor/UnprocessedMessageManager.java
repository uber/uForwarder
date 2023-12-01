package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.common.StructuredTags;
import com.uber.data.kafka.consumerproxy.worker.limiter.BootstrapLongFixedInflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.limiter.InflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.limiter.LongFixedInflightLimiter;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.RoutingUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import com.uber.m3.tally.Scope;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * UnprocessedMessageManager tracks both the number and byte size of unprocessed message of each
 * topic partition
 */
public class UnprocessedMessageManager implements BlockingQueue, MetricSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnprocessedMessageManager.class);
  protected final ConcurrentMap<TopicPartition, PartitionLimiter> topicPartitionLimiterMap;
  protected final int maxInboundCacheCount;
  protected final long maxInboundByteSize;
  protected final LongFixedInflightLimiter sharedByteSizeLimiter;
  protected final Scope scope;
  protected final Job jobTemplate;
  protected final LongFixedInflightLimiter countLimiter;
  protected final LongFixedInflightLimiter byteSizeLimiter;

  UnprocessedMessageManager(
      Job jobTemplate,
      int maxInboundCacheCount,
      long maxInboundByteSize,
      LongFixedInflightLimiter sharedByteSizeLimiter,
      Scope scope) {
    this.jobTemplate = jobTemplate;
    this.maxInboundCacheCount = maxInboundCacheCount;
    this.maxInboundByteSize = maxInboundByteSize;
    this.sharedByteSizeLimiter = sharedByteSizeLimiter;
    this.topicPartitionLimiterMap = new ConcurrentHashMap<>();
    this.scope = scope;
    this.countLimiter = BootstrapLongFixedInflightLimiter.newBuilder().withLimit(0).build();
    this.byteSizeLimiter = new LongFixedInflightLimiter(0);
  }

  @Override
  public void publishMetrics() {
    topicPartitionLimiterMap.values().stream().forEach(limiter -> limiter.publishMetrics());
  }

  void init(Job job) {
    TopicPartition topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    topicPartitionLimiterMap.computeIfAbsent(
        topicPartition, tp -> new PartitionLimiter(job, newLimiter(job)));
    syncLimit();
  }

  /**
   * cancels the semaphore for the topic-partition
   *
   * @param tp a TopicPartition
   */
  void cancel(TopicPartition tp) {
    PartitionLimiter limiter = topicPartitionLimiterMap.remove(tp);
    if (limiter != null) {
      limiter.close();
      syncLimit();
    }
  }

  /** cancels all semaphores for all topic-partitions */
  void cancelAll() {
    topicPartitionLimiterMap.values().forEach(limiter -> limiter.close());
    topicPartitionLimiterMap.clear();
    syncLimit();
  }

  @VisibleForTesting
  @Nullable
  PartitionLimiter getLimiter(TopicPartition tp) {
    PartitionLimiter limiter = topicPartitionLimiterMap.get(tp);
    if (limiter == null) {
      logAndReportUnassignedTopicPartition(tp);
    }
    return limiter;
  }

  /**
   * caches a received message
   *
   * @param pm the physical received message
   */
  void receive(ProcessorMessage pm) {
    TopicPartitionOffset topicPartitionOffset = pm.getPhysicalMetadata();
    TopicPartition tp =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    PartitionLimiter limiter = topicPartitionLimiterMap.get(tp);
    if (limiter == null) {
      logAndReportUnassignedTopicPartition(tp);
      throw new IllegalStateException(
          String.format(
              "topic-partition %s has not been assigned to this message processor", tp.toString()));
    }
    try {
      // For 0-size messages, we should just skip, or acquire will fail
      if (pm.getValueByteSize() > 0) {
        pm.setPermit(limiter.acquire(pm));
      } else {
        LOGGER.error(
            "received empty message",
            StructuredLogging.kafkaTopic(topicPartitionOffset.getTopic()),
            StructuredLogging.kafkaPartition(topicPartitionOffset.getPartition()),
            StructuredLogging.kafkaOffset(topicPartitionOffset.getOffset()));
      }
    } catch (InterruptedException e) {
      LOGGER.error(
          "the thread is interrupted while waiting on Semaphore",
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
          .counter(MetricNames.UNPROCESSED_MESSAGE_MANAGER_INTERRUPTED)
          .inc(1);
    }
  }

  /**
   * removes a message from cache
   *
   * @param pm the physical received message
   */
  void remove(ProcessorMessage pm) {
    TopicPartitionOffset topicPartitionOffset = pm.getPhysicalMetadata();
    TopicPartition tp =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    PartitionLimiter limiter = topicPartitionLimiterMap.get(tp);
    if (limiter == null) {
      logAndReportUnassignedTopicPartition(tp);
      // this topic-partition is not tracked, so do nothing
    } else {
      limiter.release(pm);
    }
  }

  /**
   * Creates limiter
   *
   * @param job
   * @return
   */
  protected Limiter newLimiter(Job job) {
    return new SharedLimiter();
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
        .counter(MetricNames.UNPROCESSED_MESSAGE_MANAGER_UNASSIGNED)
        .inc(1);
  }

  /**
   * Updates limit according to partition count when reCreate equals to true it will close existing
   * limiter to unblock threads reCreate should be true when decrease limit
   */
  private void syncLimit() {
    // release all blocked threads than create new limiter
    int nPartitions = topicPartitionLimiterMap.size();
    countLimiter.updateLimit(nPartitions * maxInboundCacheCount);
    byteSizeLimiter.updateLimit(nPartitions * maxInboundByteSize);
  }

  @Override
  public Optional<BlockingMessage> detectBlockingMessage(TopicPartition topicPartition) {
    return Optional.empty();
  }

  @Override
  public boolean markCanceled(TopicPartitionOffset topicPartitionOffset) {
    return false;
  }

  protected static Scope jobScope(Scope scope, Job job) {
    final String group = job.getKafkaConsumerTask().getConsumerGroup();
    final String cluster = job.getKafkaConsumerTask().getCluster();
    final String topic = job.getKafkaConsumerTask().getTopic();
    final String partition = Integer.toString(job.getKafkaConsumerTask().getPartition());
    final String routingKey = RoutingUtils.extractAddress(job.getRpcDispatcherTask().getUri());
    return scope.tagged(
        ImmutableMap.of(
            StructuredFields.KAFKA_GROUP,
            group,
            StructuredFields.KAFKA_CLUSTER,
            cluster,
            StructuredFields.KAFKA_TOPIC,
            topic,
            StructuredFields.KAFKA_PARTITION,
            partition,
            StructuredFields.URI,
            routingKey));
  }

  /** Composition of permits */
  protected class NestedPermit implements InflightLimiter.Permit {
    private AtomicBoolean completed;
    private final List<InflightLimiter.Permit> permits;

    /**
     * Creates a nested permit with nested permits
     *
     * @param permits
     */
    NestedPermit(InflightLimiter.Permit... permits) {
      this.permits = ImmutableList.copyOf(permits);
      this.completed = new AtomicBoolean(false);
    }

    @Override
    public boolean complete(InflightLimiter.Result result) {
      if (completed.compareAndSet(false, true)) {
        permits.stream().forEach(permit -> permit.complete(result));
        return true;
      }
      return false;
    }
  }

  interface Limiter extends MetricSource {
    default boolean close() {
      return true;
    }

    InflightLimiter.Permit acquire(ProcessorMessage pm) throws InterruptedException;

    /**
     * message count limit of the limiter
     *
     * @return
     */
    long countLimit();
  }

  /** Limiter of a partition */
  class PartitionLimiter implements Limiter {
    private final AtomicLong totalMessages;
    private final AtomicLong totalBytes;
    private final Set<InflightLimiter.Permit> permits;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Scope jobScope;
    private final Limiter delegator;

    private PartitionLimiter(Job job, Limiter delegator) {
      this.totalMessages = new AtomicLong();
      this.totalBytes = new AtomicLong();
      this.permits = ConcurrentHashMap.newKeySet();
      this.jobScope = jobScope(scope, job);
      this.delegator = delegator;
    }

    @Override
    public boolean close() {
      if (isClosed.compareAndSet(false, true)) {
        // for permits get from shared limiter, proactively release them to prevent leak of quota
        // as global limiter won't be closed with job
        permits.forEach(permit -> permit.complete());
        permits.clear();
        delegator.close();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public InflightLimiter.Permit acquire(ProcessorMessage pm) throws InterruptedException {
      int byteSize = pm.getValueByteSize();
      totalMessages.incrementAndGet();
      totalBytes.addAndGet(byteSize);
      InflightLimiter.Permit result = delegator.acquire(pm);
      permits.add(result);
      return result;
    }

    @Override
    public long countLimit() {
      return delegator.countLimit();
    }

    @VisibleForTesting
    protected boolean isClosed() {
      return isClosed.get();
    }

    private void release(ProcessorMessage pm) {
      int byteSize = pm.getValueByteSize();
      pm.getPermit()
          .ifPresent(
              permit -> {
                if (permit.complete()) {
                  totalMessages.decrementAndGet();
                  totalBytes.addAndGet(-byteSize);
                  permits.remove(permit);
                }
              });
    }

    @Override
    public void publishMetrics() {
      jobScope.gauge(MetricNames.PROCESSOR_MESSAGES_COUNT).update(totalMessages.get());
      jobScope.gauge(MetricNames.PROCESSOR_MESSAGES_BYTES).update(totalBytes.get());
      jobScope
          .gauge(MetricNames.PROCESSOR_MESSAGES_GLOBAL_BYTES)
          .update(sharedByteSizeLimiter.getMetrics().getInflight());
      delegator.publishMetrics();
    }

    protected Limiter getDelegator() {
      return delegator;
    }

    @VisibleForTesting
    public boolean validate() {
      return totalMessages.get() == permits.size();
    }
  }

  protected class SharedLimiter implements Limiter {

    @Override
    public InflightLimiter.Permit acquire(ProcessorMessage pm) throws InterruptedException {
      int byteSize = pm.getValueByteSize();
      InflightLimiter.Permit countPermit = countLimiter.acquire();
      Optional<InflightLimiter.Permit> permit = byteSizeLimiter.tryAcquire(byteSize);
      InflightLimiter.Permit result;
      if (permit.isPresent()) {
        result = new NestedPermit(countPermit, permit.get());
      } else {
        try {
          result = new NestedPermit(countPermit, sharedByteSizeLimiter.acquire(byteSize));
        } catch (InterruptedException e) {
          // release allocated permit
          countPermit.complete();
          throw e;
        }
      }

      return result;
    }

    @Override
    public long countLimit() {
      return countLimiter.getMetrics().getLimit();
    }
  }

  public static class Builder {
    protected final int maxInboundCacheCount;
    protected final long maxInboundByteSize;
    protected final LongFixedInflightLimiter sharedByteSizeLimiter;
    protected final CoreInfra infra;

    public Builder(
        int maxInboundCacheCount,
        long maxInboundByteSize,
        LongFixedInflightLimiter sharedByteSizeLimiter,
        CoreInfra infra) {
      this.maxInboundCacheCount = maxInboundCacheCount;
      this.maxInboundByteSize = maxInboundByteSize;
      this.sharedByteSizeLimiter = sharedByteSizeLimiter;
      this.infra = infra;
    }

    UnprocessedMessageManager build(Job job) {
      return new UnprocessedMessageManager(
          job, maxInboundCacheCount, maxInboundByteSize, sharedByteSizeLimiter, infra.scope());
    }
  }

  static class MetricNames {
    static final String UNPROCESSED_MESSAGE_MANAGER_UNASSIGNED =
        "processor.unprocessed-message-manager.unassigned";
    static final String UNPROCESSED_MESSAGE_MANAGER_INTERRUPTED =
        "processor.unprocessed-message-manager.interrupted";
    // Total messages in processor buffer
    static final String PROCESSOR_MESSAGES_COUNT = "processor.messages.count";
    // Total bytes in processor buffer
    static final String PROCESSOR_MESSAGES_BYTES = "processor.messages.bytes";
    // Total bytes in processor buffer
    static final String PROCESSOR_MESSAGES_GLOBAL_BYTES = "processor.messages.global.bytes";
  }
}
