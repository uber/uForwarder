package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.data.kafka.datatransfer.controller.rpc.JobWorkloadSink;
import com.uber.data.kafka.datatransfer.controller.rpc.Workload;
import com.uber.m3.tally.Scope;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * JobThroughputMonitor tracks throughput of each topic consumer. Throughput is provided by
 * heartbeat request of works, then consumed by AutoScalar Internal status cleanup automatically
 * after ttl since last access
 */
public class JobWorkloadMonitor implements JobWorkloadSink {
  private final LoadingCache<JobGroupKey, JobGroupWorkload> jobGroupWorkloadStore;
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
  public JobWorkloadMonitor(AutoScalarConfiguration config, Ticker ticker, Scope scope) {
    this.config = config;
    this.jobGroupWorkloadStore =
        CacheBuilder.newBuilder()
            .expireAfterAccess(config.getJobStatusTTL().toMillis(), TimeUnit.MILLISECONDS)
            .ticker(ticker)
            .build(
                new CacheLoader<>() {
                  @Override
                  public JobGroupWorkload load(JobGroupKey key) {
                    return new JobGroupWorkload();
                  }
                });
    this.ticker = ticker;
    this.scope = scope;
  }

  /**
   * updates throughput if JobGroupThroughput presents
   *
   * @param job the job
   * @param workload the workload
   */
  @Override
  public void consume(Job job, Workload workload) {
    JobGroupKey jobGroupKey = JobGroupKey.of(job);
    int partition = job.getKafkaConsumerTask().getPartition();
    Scope taggedScope =
        scope.tagged(
            StructuredTags.builder()
                .setKafkaGroup(jobGroupKey.getGroup())
                .setKafkaTopic(jobGroupKey.getTopic())
                .setKafkaPartition(partition)
                .build());
    taggedScope
        .gauge(MetricNames.JOB_THROUGHPUT_MESSAGES_PER_SECOND)
        .update(workload.getMessagesPerSecond());
    taggedScope
        .gauge(MetricNames.JOB_THROUGHPUT_BYTES_PER_SECOND)
        .update(workload.getBytesPerSecond());

    jobGroupWorkloadStore.getUnchecked(jobGroupKey).put(partition, workload);
  }

  /**
   * Gets workload metrics of a job group if present
   *
   * @param jobGroupKey the job group key
   * @return the workload
   */
  public Optional<Workload> get(JobGroupKey jobGroupKey) {
    return jobGroupWorkloadStore.getUnchecked(jobGroupKey).getSum();
  }

  @VisibleForTesting
  protected Map<JobGroupKey, JobGroupWorkload> getJobGroupWorkloadMap() {
    return jobGroupWorkloadStore.asMap();
  }

  @VisibleForTesting
  protected void cleanUp() {
    // clean up job group throughput store
    jobGroupWorkloadStore.cleanUp();
    // for what ever left, clean up partition throughput store
    jobGroupWorkloadStore.asMap().forEach((key, value) -> value.cleanUp());
  }

  /** Stores throughput of each job of the job group */
  protected class JobGroupWorkload {
    private Cache<Integer, Workload> partitionWorkloadStore;

    JobGroupWorkload() {
      partitionWorkloadStore =
          CacheBuilder.newBuilder()
              .expireAfterWrite(config.getThroughputTTL().toNanos(), TimeUnit.NANOSECONDS)
              .ticker(ticker)
              .build();
    }

    /**
     * Updates workload
     *
     * @param partition the partition
     * @param workload the workload
     */
    void put(int partition, Workload workload) {
      partitionWorkloadStore.put(partition, workload);
    }

    /**
     * Retrieves throughput of a jobGroup or empty if no throughput present
     *
     * @return the throughput
     */
    synchronized Optional<Workload> getSum() {
      double messagesPerSecond = 0.0;
      double bytesPerSecond = 0.0;
      double cpuUsage = 0.0;
      boolean present = false;
      for (Map.Entry<Integer, Workload> entry : partitionWorkloadStore.asMap().entrySet()) {
        Workload workload = entry.getValue();
        messagesPerSecond += workload.getMessagesPerSecond();
        bytesPerSecond += workload.getBytesPerSecond();
        cpuUsage += workload.getCpuUsage();
        present = true;
      }
      if (present) {
        return Optional.of(Workload.of(messagesPerSecond, bytesPerSecond, cpuUsage));
      } else {
        return Optional.empty();
      }
    }

    private void cleanUp() {
      partitionWorkloadStore.cleanUp();
    }
  }

  private static class MetricNames {
    private static final String JOB_THROUGHPUT_MESSAGES_PER_SECOND =
        "job.throughput.messagesPerSecond";
    private static final String JOB_THROUGHPUT_BYTES_PER_SECOND = "job.throughput.bytesPerSecond";
  }
}
