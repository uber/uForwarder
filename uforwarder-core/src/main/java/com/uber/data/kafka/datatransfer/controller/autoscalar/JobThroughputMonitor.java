package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.data.kafka.datatransfer.controller.rpc.JobThroughputSink;
import com.uber.m3.tally.Scope;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * JobThroughputMonitor tracks throughput of each topic consumer. Throughput is provided by
 * heartbeat request of works, then consumed by AutoScalar Internal status cleanup automatically
 * after ttl since last access
 */
public class JobThroughputMonitor implements JobThroughputSink {
  private final LoadingCache<JobGroupKey, JobGroupThroughput> jobGroupThroughputStore;
  private final AutoScalarConfiguration config;
  private final Ticker ticker;
  private final Scope scope;

  /**
   * Instantiates a new JobThroughputMonitor
   *
   * @param config the config
   * @param ticker the ticker
   * @param scope the scope
   */
  public JobThroughputMonitor(AutoScalarConfiguration config, Ticker ticker, Scope scope) {
    this.config = config;
    this.jobGroupThroughputStore =
        CacheBuilder.newBuilder()
            .expireAfterAccess(config.getJobStatusTTL().toMillis(), TimeUnit.MILLISECONDS)
            .ticker(ticker)
            .build(
                new CacheLoader<>() {
                  @Override
                  public JobGroupThroughput load(JobGroupKey key) {
                    return new JobGroupThroughput();
                  }
                });
    this.ticker = ticker;
    this.scope = scope;
  }

  /**
   * updates throughput if JobGroupThroughput presents
   *
   * @param job the job
   * @param messagesPerSecond the messages per second
   * @param bytesPerSecond the bytes per second
   */
  @Override
  public void consume(Job job, double messagesPerSecond, double bytesPerSecond) {
    JobGroupKey jobGroupKey = JobGroupKey.of(job);
    int partition = job.getKafkaConsumerTask().getPartition();
    Scope taggedScope =
        scope.tagged(
            StructuredTags.builder()
                .setKafkaGroup(jobGroupKey.getGroup())
                .setKafkaTopic(jobGroupKey.getTopic())
                .setKafkaPartition(partition)
                .build());
    taggedScope.gauge(MetricNames.JOB_THROUGHPUT_MESSAGES_PER_SECOND).update(messagesPerSecond);
    taggedScope.gauge(MetricNames.JOB_THROUGHPUT_BYTES_PER_SECOND).update(bytesPerSecond);

    jobGroupThroughputStore
        .getUnchecked(jobGroupKey)
        .put(
            job.getKafkaConsumerTask().getPartition(),
            Throughput.of(messagesPerSecond, bytesPerSecond));
  }

  /**
   * Gets throughput of a job group if present
   *
   * @param jobGroupKey the job group key
   * @return the throughput
   */
  public Optional<Throughput> get(JobGroupKey jobGroupKey) {
    return jobGroupThroughputStore.getUnchecked(jobGroupKey).getSum();
  }

  @VisibleForTesting
  protected Map<JobGroupKey, JobGroupThroughput> getJobGroupThroughputMap() {
    return jobGroupThroughputStore.asMap();
  }

  @VisibleForTesting
  protected void cleanUp() {
    // clean up job group throughput store
    jobGroupThroughputStore.cleanUp();
    // for what ever left, clean up partition throughput store
    jobGroupThroughputStore.asMap().forEach((key, value) -> value.cleanUp());
  }

  /** Stores throughput of each job of the job group */
  protected class JobGroupThroughput {
    private Cache<Integer, Throughput> partitionThroughputStore;

    JobGroupThroughput() {
      partitionThroughputStore =
          CacheBuilder.newBuilder()
              .expireAfterWrite(config.getThroughputTTL().toNanos(), TimeUnit.NANOSECONDS)
              .ticker(ticker)
              .build();
    }

    /**
     * Updates throughput
     *
     * @param partition the partition
     * @param throughput the throughput
     */
    void put(int partition, Throughput throughput) {
      partitionThroughputStore.put(partition, throughput);
    }

    /**
     * Retrieves throughput of a jobGroup or empty if no throughput present
     *
     * @return the throughput
     */
    synchronized Optional<Throughput> getSum() {
      double messagesPerSecond = 0.0;
      double bytesPerSecond = 0.0;
      boolean present = false;
      for (Map.Entry<Integer, Throughput> entry : partitionThroughputStore.asMap().entrySet()) {
        Throughput throughput = entry.getValue();
        messagesPerSecond += throughput.getMessagesPerSecond();
        bytesPerSecond += throughput.getBytesPerSecond();
        present = true;
      }
      if (present) {
        return Optional.of(Throughput.of(messagesPerSecond, bytesPerSecond));
      } else {
        return Optional.empty();
      }
    }

    private void cleanUp() {
      partitionThroughputStore.cleanUp();
    }
  }

  private static class MetricNames {
    private static final String JOB_THROUGHPUT_MESSAGES_PER_SECOND =
        "job.throughput.messagesPerSecond";
    private static final String JOB_THROUGHPUT_BYTES_PER_SECOND = "job.throughput.bytesPerSecond";
  }
}
