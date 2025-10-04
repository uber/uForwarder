package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

/**
 * AbstractAckTrackingQueue provides common implementation of ${@link ArrayAckTrackingQueue} and
 * ${@link LinkedAckTrackingQueue}
 */
public abstract class AbstractAckTrackingQueue implements AckTrackingQueue {
  protected static final long INITIAL_OFFSET = -1;

  /** Size of the AckStatus array */
  protected final int capacity;

  protected final Job job;

  protected final Scope scope;

  protected final Logger logger;

  protected final RuntimeStats runtimeStats;

  protected final Map<AttributeKey, Map<Attribute, RuntimeStats>> attributeRuntimeStatsMap;

  protected final TopicPartition topicPartition;

  /** a flag marking whether the current queue is still in use or not */
  volatile boolean notInUse;

  volatile StateImpl state;

  /** Main lock guarding all access */
  final ReentrantLock lock;

  /** Condition for waiting adds */
  final Condition notFull;

  final List<Reactor> reactors;

  AbstractAckTrackingQueue(Job job, int capacity, Scope jobScope, Logger logger) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("the capacity is negative");
    }

    this.job = job;
    this.topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    this.capacity = capacity;
    this.scope = jobScope;
    this.logger = logger;
    this.runtimeStats = new RuntimeStats();
    this.attributeRuntimeStatsMap = new ConcurrentHashMap<>();
    // initialize those non-changeable variables
    this.lock = new ReentrantLock(false);
    this.notFull = this.lock.newCondition();
    this.reactors = new ArrayList<>();
    this.notInUse = false;
    this.scope.gauge(MetricNames.IN_MEMORY_CAPACITY).update(capacity);
    this.state = new StateImpl(capacity, INITIAL_OFFSET);
  }

  @Override
  public void receive(long offset) throws InterruptedException {
    receive(offset, ImmutableMap.of());
  }

  /**
   * When a commit tracking queue is not in use anymore, it needs to be marked as invalid.
   * Specifically, this function sets the invalid flag to be true, and unblocks all blocked threads.
   */
  @Override
  public void markAsNotInUse() {
    notInUse = true;
    lock.lock();
    try {
      notFull.signalAll();
      logger.info(
          "an ack tracking queue was marked as not in use",
          StructuredLogging.jobId(job.getJobId()),
          StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
          StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
          StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
          StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  @Override
  public void publishMetrics() {
    scope.gauge(MetricNames.IN_MEMORY_UNCOMMITTED).update(state.stats.size());
    scope.gauge(MetricNames.IN_MEMORY_CAPACITY).update(capacity);
    scope.gauge(MetricNames.LOAD_FACTOR).update(state.loadFactor(false));
    scope.gauge(MetricNames.LOAD_FACTOR_EXCLUSIVE).update(state.loadFactor(true));
    scope.gauge(MetricNames.ACKED_COUNT).update(state.stats().acked());
    scope.gauge(MetricNames.CANCELED_COUNT).update(state.stats().canceled());
  }

  @Override
  public void addReactor(Reactor reactor) {
    reactors.add(reactor);
  }

  @Override
  public abstract void receive(long offset, Map<AttributeKey, Attribute> attributes)
      throws InterruptedException;

  /**
   * tests if AckTrackingQueue is full
   *
   * @param offset the offset to mark
   * @return the boolean
   */
  protected abstract boolean isFull(long offset);

  /**
   * Try to wait for the queue to be not full.
   *
   * @param offsetToMark the offset to be marked
   * @return false when the wait fails, and the caller should return; true when the wait succeeds,
   *     and the caller should proceed to do other work
   * @throws InterruptedException when the current thread is waiting for the signal, it may be
   *     interrupted.
   */
  protected boolean waitForNotFull(long offsetToMark) throws InterruptedException {
    // Check whether the ack tracking is marked as not in use or not.
    // If it's not in use any more, there is no need to wait for the ack tracking is not full,
    // simply tell the caller that it cannot acquire the lock.
    if (notInUse) {
      return false;
    }
    Stopwatch stopwatch = null;
    lock.lock();
    try {
      while (isFull(offsetToMark)) {
        if (notInUse) {
          if (stopwatch != null) {
            stopwatch.stop();
          }
          logger.info(
              "an ack tracking queue is not in use now",
              StructuredLogging.jobId(job.getJobId()),
              StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
              StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
              StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
              StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()));
          return false;
        } else {
          if (stopwatch == null) {
            stopwatch =
                scope.timer(AckTrackingQueue.MetricNames.STATUS_CHANGE_WAITING_TIME).start();
          }
          notFull.await();
        }
      }
      if (stopwatch != null) {
        stopwatch.stop();
      }
      logger.debug(
          "an ack tracking queue is not full now",
          StructuredLogging.jobId(job.getJobId()),
          StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
          StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
          StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
          StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()));
      return true;
    } finally {
      lock.unlock();
    }
  }

  protected void onStatusUpdate(
      AckStatus preAckStatus, AckStatus ackStatus, Map<AttributeKey, Attribute> key) {
    runtimeStats.onUpdate(preAckStatus, ackStatus);
    for (Map.Entry<AttributeKey, Attribute> entry : key.entrySet()) {
      RuntimeStats attributeRuntimeStats =
          attributeRuntimeStatsMap
              .getOrDefault(entry.getKey(), ImmutableMap.of())
              .get(entry.getValue());
      if (attributeRuntimeStats != null) {
        attributeRuntimeStats.onUpdate(preAckStatus, ackStatus);
      } else {
        throw new RuntimeException(
            "attributeRuntimeStats should have already been initialized when receive");
      }
    }
  }

  protected void onReceive(Map<AttributeKey, Attribute> attributes) {
    runtimeStats.onReceive();
    for (Map.Entry<AttributeKey, Attribute> entry : attributes.entrySet()) {
      attributeRuntimeStatsMap
          .computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
          .computeIfAbsent(entry.getValue(), k -> new RuntimeStats())
          .onReceive();
    }
  }

  protected void resetRuntimeStats() {
    runtimeStats.reset();
    attributeRuntimeStatsMap.clear();
  }

  /** Mutable statistics */
  private class RuntimeStats {
    /** number of offset acked in memory */
    private volatile int acked;

    /** number of offset canceled in memory */
    private volatile int canceled;

    /** number of offset in memory */
    private volatile int size;

    void reset() {
      this.acked = 0;
      this.canceled = 0;
      this.size = 0;
    }

    void onReceive() {
      size++;
    }

    void onUpdate(AckStatus oldStatus, AckStatus ackStatus) {
      if (ackStatus == AckStatus.ACKED) {
        acked++;
      } else if (oldStatus == AckStatus.ACKED) {
        acked--;
      }

      if (ackStatus == AckStatus.CANCELED) {
        canceled++;
      } else if (oldStatus == AckStatus.CANCELED) {
        canceled--;
      }

      if (ackStatus == AckStatus.UNSET) {
        size--;
      }
    }
  }

  /** State of the {@link AckTrackingQueue} */
  protected class StateImpl implements State {
    private final int capacity;
    private final Stats stats;
    private final long headOffset;
    private final long tailOffset;
    private final long lowestCancelableOffset;
    private final long highestAckedOffset;
    private final Map<AttributeKey, Map<Attribute, Stats>> attributeStats;

    StateImpl(int capacity, long highestAckedOffset) {
      this(capacity, 0, INITIAL_OFFSET, INITIAL_OFFSET, INITIAL_OFFSET, highestAckedOffset);
    }

    StateImpl(
        int capacity,
        int size,
        long headOffset,
        long tailOffset,
        long lowestCancelableOffset,
        long highestAckedOffset) {
      this.capacity = capacity;
      this.stats = new StatsImpl(size, runtimeStats.acked, runtimeStats.canceled);
      this.headOffset = headOffset;
      this.tailOffset = tailOffset;
      this.lowestCancelableOffset = lowestCancelableOffset;
      this.highestAckedOffset = highestAckedOffset;
      this.attributeStats =
          attributeRuntimeStatsMap.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      entry ->
                          entry.getValue().entrySet().stream()
                              .collect(
                                  Collectors.toMap(
                                      Map.Entry::getKey,
                                      e ->
                                          new StatsImpl(
                                              e.getValue().size,
                                              e.getValue().acked,
                                              e.getValue().canceled)))));
    }

    @Override
    public int capacity() {
      return capacity;
    }

    @Override
    public Stats stats() {
      return stats;
    }

    @Override
    public long headOffset() {
      return headOffset;
    }

    @Override
    public long tailOffset() {
      return tailOffset;
    }

    @Override
    public long lowestCancelableOffset() {
      return lowestCancelableOffset;
    }

    @Override
    public long highestAckedOffset() {
      return highestAckedOffset;
    }

    @Override
    public Map<AttributeKey, Map<Attribute, Stats>> attributesStats() {
      return attributeStats;
    }

    @Override
    public double loadFactor(boolean excludeCanceled) {
      if (capacity == 0) {
        return 0.0d;
      }

      int size = this.stats.size() - (excludeCanceled ? state.stats().canceled() : 0);
      return size / (double) capacity;
    }

    @Override
    public String toString() {
      return "StateImpl{"
          + "capacity="
          + capacity
          + ", stats="
          + stats
          + ", headOffset="
          + headOffset
          + ", tailOffset="
          + tailOffset
          + ", lowestCancelableOffset="
          + lowestCancelableOffset
          + ", highestAckedOffset="
          + highestAckedOffset
          + ", attributesStats="
          + attributeStats
          + '}';
    }
  }

  private class StatsImpl implements Stats {
    private final int size;
    private final int acked;
    private final int canceled;

    StatsImpl(int size, int acked, int canceled) {
      this.size = size;
      this.acked = acked;
      this.canceled = canceled;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int acked() {
      return acked;
    }

    @Override
    public int canceled() {
      return canceled;
    }

    @Override
    public String toString() {
      return "StatsImpl{" + "size=" + size + ", acked=" + acked + ", canceled=" + canceled + '}';
    }
  }
}
