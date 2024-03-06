package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.kafka.common.TopicPartition;

@ThreadSafe
/** This class tracks the stats of inflight messages for each topic partition */
public class InflightMessageTracker {
  private static final InflightMessageStats ZERO = new InflightMessageStats(0, 0);
  private final ConcurrentMap<TopicPartition, InflightMessageStats> inflightStatsMap;

  public InflightMessageTracker() {
    inflightStatsMap = new ConcurrentHashMap<>();
  }

  /**
   * Initialize for a topicPartition
   *
   * @param topicPartition The topicPartition to initialize
   */
  public void init(TopicPartition topicPartition) {
    inflightStatsMap.putIfAbsent(topicPartition, new InflightMessageStats(0, 0));
  }

  /**
   * Add a message from a topicPartition to the tracker
   *
   * @param topicPartition The topicPartition where the message belongs to
   * @param messageSize The size of the message
   */
  public void addMessage(TopicPartition topicPartition, int messageSize) {
    inflightStatsMap.computeIfPresent(
        topicPartition,
        (tp, stats) -> {
          stats.addMessage(messageSize);
          return stats;
        });
  }

  /**
   * Removes a message from a job from the tracker
   *
   * @param topicPartition The topicPartition where the message belongs to
   * @param messageSize The size of the message
   */
  public void removeMessage(TopicPartition topicPartition, int messageSize) {
    inflightStatsMap.computeIfPresent(
        topicPartition,
        (tp, stats) -> {
          stats.removeMessage(messageSize);
          return stats;
        });
  }

  /**
   * Revokes the stats for a topicPartition
   *
   * @param topicPartition The topicPartition to revoke stats
   */
  public void revokeInflightStatsForJob(TopicPartition topicPartition) {
    inflightStatsMap.remove(topicPartition);
  }

  /** Clears all stats */
  public void clear() {
    inflightStatsMap.clear();
  }

  /**
   * Gets the inflight message stats for a topic partition
   *
   * @param tp
   * @return
   */
  public InflightMessageStats getInflightMessageStats(TopicPartition tp) {
    return inflightStatsMap.getOrDefault(tp, ZERO);
  }

  @ThreadSafe
  static class InflightMessageStats {
    final AtomicInteger numberOfMessages;
    final AtomicLong totalBytes;

    InflightMessageStats(int numberOfMessages, int totalBytes) {
      this.numberOfMessages = new AtomicInteger(numberOfMessages);
      this.totalBytes = new AtomicLong(totalBytes);
    }

    void addMessage(int messageSizeInBytes) {
      numberOfMessages.incrementAndGet();
      totalBytes.addAndGet(messageSizeInBytes);
    }

    void removeMessage(int messageSizeInBytes) {
      numberOfMessages.decrementAndGet();
      totalBytes.addAndGet(-messageSizeInBytes);
    }
  }
}
