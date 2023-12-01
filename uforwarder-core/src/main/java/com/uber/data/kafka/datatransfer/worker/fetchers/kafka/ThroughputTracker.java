package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.google.common.base.Ticker;
import com.uber.concurrency.loadbalancer.metrics.Meter;
import com.uber.data.kafka.datatransfer.Job;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.common.TopicPartition;

/** ThroughputTracker track and report message/byte throughput of topic partition */
public class ThroughputTracker {
  private final ConcurrentMap<TopicPartition, M1Rate> tpAckRate;
  private final Ticker ticker;

  /** Instantiates a new Throughput tracker. */
  public ThroughputTracker() {
    this(Ticker.systemTicker());
  }

  /**
   * Instantiates a new Throughput tracker.
   *
   * @param ticker the ticker
   */
  ThroughputTracker(Ticker ticker) {
    this.tpAckRate = new ConcurrentHashMap<>();
    this.ticker = ticker;
  }

  /** Starts to track a topic partition */
  public void init(Job job) {
    TopicPartition topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    tpAckRate.computeIfAbsent(topicPartition, o -> new M1Rate());
  }

  /** Cancel all. */
  public void clear() {
    tpAckRate.clear();
  }

  /**
   * updates message/byte rate when message get ack
   *
   * @param job the job
   * @param messages the messages
   * @param bytes the bytes
   */
  void record(Job job, int messages, int bytes) {
    TopicPartition topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    tpAckRate.computeIfPresent(
        topicPartition,
        (tp, meter) -> {
          meter.mark(messages, bytes);
          return meter;
        });
  }

  /**
   * Gets throughput.
   *
   * @param job the job
   * @return the throughput
   */
  protected Throughput getThroughput(Job job) {
    TopicPartition topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    M1Rate result = tpAckRate.get(topicPartition);
    if (result == null) {
      return Throughput.ZERO;
    } else {
      return new Throughput(result.messageRate.getRate(), result.bytesRate.getRate());
    }
  }

  static class Throughput {
    private static Throughput ZERO = new Throughput(0, 0);
    final double messagePerSec;
    final double bytesPerSec;

    Throughput(double messagePerSec, double bytesPerSec) {
      this.messagePerSec = messagePerSec;
      this.bytesPerSec = bytesPerSec;
    }
  }

  /** One minute EWMA rate */
  private class M1Rate {

    /** The Message rate. */
    Meter messageRate = new Meter(ticker);
    /** The Bytes rate. */
    Meter bytesRate = new Meter(ticker);

    /**
     * Marks ack of message
     *
     * @param messages the messages
     * @param bytes the bytes
     */
    void mark(int messages, int bytes) {
      messageRate.mark(messages);
      bytesRate.mark(bytes);
    }
  }
}
