package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.common.StructuredTags;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.AdminClient;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BatchRpcUriRebalancer extends {@link AbstractRpcUriRebalancer}.
 *
 * <p>It first updates job state to {@link JobState#JOB_STATE_CANCELED} if
 *
 * <ol>
 *   <li>the job is not in canceled state
 *   <li>the committed offset reported by the worker has reached the end offset specified in the job
 *       configuration
 * </ol>
 *
 * It then updates job state to the job group state if
 *
 * <ol>
 *   <li>the job is not in canceled state
 *   <li>the job is in a different state
 * </ol>
 */
public class BatchRpcUriRebalancer extends AbstractRpcUriRebalancer {
  private static final long OFFSET_COMMIT_SKEW_MS = TimeUnit.HOURS.toMillis(1);

  private static final Logger logger = LoggerFactory.getLogger(BatchRpcUriRebalancer.class);
  private final DynamicConfiguration dynamicConfiguration;
  private final AdminClient.Builder adminBuilder;
  private final Scope scope;

  public BatchRpcUriRebalancer(
      Scope scope,
      RebalancerConfiguration config,
      Scalar scalar,
      HibernatingJobRebalancer hibernatingJobRebalancer,
      AdminClient.Builder adminBuilder,
      DynamicConfiguration dynamicConfiguration)
      throws IOException {
    super(scope, config, scalar, hibernatingJobRebalancer);
    this.scope = scope.tagged(ImmutableMap.of("rebalancerType", "BatchRpcUriRebalancer"));
    this.dynamicConfiguration = dynamicConfiguration;
    this.adminBuilder = adminBuilder;
  }

  @Override
  public void computeJobState(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {
    for (RebalancingJobGroup rebalancingJobGroup : jobGroups.values()) {
      // although the job group will be eventually canceled, we check start and end timestamps here
      // to make sure it happens early.
      KafkaConsumerTaskGroup kafkaConsumerTaskGroup =
          rebalancingJobGroup.getJobGroup().getKafkaConsumerTaskGroup();
      if (kafkaConsumerTaskGroup
          .getStartTimestamp()
          .equals(kafkaConsumerTaskGroup.getEndTimestamp())) {
        rebalancingJobGroup.updateJobGroupState(JobState.JOB_STATE_CANCELED);
      }
      JobState jobGroupState = rebalancingJobGroup.getJobGroupState();
      Map<Long, StoredJobStatus> jobStatusMap = rebalancingJobGroup.getJobStatusMap();
      boolean allJobsCanceled = true;
      for (Map.Entry<Long, StoredJob> jobEntry : rebalancingJobGroup.getJobs().entrySet()) {
        long jobId = jobEntry.getKey();
        StoredJob job = jobEntry.getValue();
        StoredJobStatus jobStatus = jobStatusMap.get(jobId);

        // One of the following (in priority order) can apply:
        // 1. Cancel the job if it has reached the end offset and it is not yet canceled, or
        //    the job is a purge request(start == end). We have to cancel existing merge to ensure
        //    that a purge can succeed in the OFFSET_COMMIT_SKEW_MS time window.
        // 2. Propagate job group state to job state only if job state is not canceled.
        //    In particular, a multiple jobs within a job group may enters CANCELED state
        //    at different times when each reaches its own end offset, so we don't want to
        //    re-run those jobs.
        if (job.getState() != JobState.JOB_STATE_CANCELED
            && jobStatus != null
            && (job.getJob().getKafkaConsumerTask().getEndOffset()
                <= jobStatus.getJobStatus().getKafkaConsumerTaskStatus().getCommitOffset())) {
          // 1. Cancel the job if it has reached the end offset and it is not yet canceled.
          job = job.toBuilder().setState(JobState.JOB_STATE_CANCELED).build();
          rebalancingJobGroup.updateJob(jobId, job);
        } else if (job.getState() != JobState.JOB_STATE_CANCELED
            && job.getState() != jobGroupState) {
          // 2. propagate job group state to job state only if job state is not CANCELED b/c
          // job state CANCELED means it reached the end offset but is waiting for other partitions
          // to reach end offset.
          rebalancingJobGroup.updateJob(jobId, job.toBuilder().setState(jobGroupState).build());
        }

        // check job state after job state change.
        allJobsCanceled &= job.getState() == JobState.JOB_STATE_CANCELED;
      }

      // If all jobs are canceled but the job group is not canceled, cancel the job group.
      if (
      // when creating new job group, it will be init without jobs; we should not cancel in that
      // case.
      rebalancingJobGroup.getJobs().size() > 0
          &&
          // only cancel job group if all jobs are canceled.
          allJobsCanceled
          &&
          // don't recancel jobs that are already canceled.
          jobGroupState != JobState.JOB_STATE_CANCELED) {
        rebalancingJobGroup.updateJobGroupState(JobState.JOB_STATE_CANCELED);
      }
    }
  }

  /**
   * Performs some operation after all other operations. In this specific case, the master needs to
   * commit offsets for jobs with the same start and end offset, because those jobs will not be
   * assigned to workers. Two known cases:
   *
   * <ol>
   *   <li>DLQ purge, which sets start timestamp and end timestamp to be the same, so the
   *       corresponding jobs will have the same start and end offsets.
   *   <li>DLQ merge, which sets start timestamp and end timestamp to be different, but when we try
   *       to create jobs, they might be mapped to the same start and end offsets.
   * </ol>
   *
   * @param jobGroups that are registered to this data-transfer cluster.
   * @param workers is the set of live workers
   */
  @Override
  public void postProcess(
      Map<String, RebalancingJobGroup> jobGroups, Map<Long, StoredWorker> workers) {
    // Dynamic config for turning it off. Although from the comments below we know it's safe to turn
    // off, we want to have a way for quick mitigation, if anything happens in production.
    if (!dynamicConfiguration.isOffsetCommittingEnabled()) {
      return;
    }
    Stopwatch postProcessStopwatch = scope.timer(MetricNames.POST_PROCESS_LATENCY).start();
    try {
      long currentTimeMs = System.currentTimeMillis();
      for (RebalancingJobGroup rebalancingJobGroup : jobGroups.values()) {
        Timestamp endTimestamp =
            rebalancingJobGroup.getJobGroup().getKafkaConsumerTaskGroup().getEndTimestamp();
        long endTimestampMs = Timestamps.toMillis(endTimestamp);
        // try to keep committing offsets for a certain time period so that it can eventually
        // succeed.
        // (1) we keep committing for a certain time period because the committed offset might be
        //     override by another purge/merge operation, which will eventually be deleted.
        // (2) we don't keep committing forever because
        //     (a) messages will be purged after a certain time period, we don't need to do it
        // anymore
        //     (b) there might be too many committing work, which might take unacceptable long time.
        if (currentTimeMs - endTimestampMs > OFFSET_COMMIT_SKEW_MS) {
          continue;
        }
        // TODO(qichao): https://t3.uberinternal.com/browse/KAFEP-1263
        // The following code for committing the offset is no longer needed due to
        // recent fixes in KCP DLQ purge. KCP now uses worker to commit the offset.
        String topic = rebalancingJobGroup.getJobGroup().getKafkaConsumerTaskGroup().getTopic();
        Map<TopicPartition, OffsetAndMetadata> partitionAndOffsetToCommit = new HashMap<>();
        for (StoredJob job : rebalancingJobGroup.getJobs().values()) {
          long startOffset = job.getJob().getKafkaConsumerTask().getStartOffset();
          long endOffset = job.getJob().getKafkaConsumerTask().getEndOffset();
          if (endOffset > 0
              && startOffset == endOffset
              && job.getState() == JobState.JOB_STATE_CANCELED) {
            partitionAndOffsetToCommit.put(
                new TopicPartition(topic, job.getJob().getKafkaConsumerTask().getPartition()),
                new OffsetAndMetadata(endOffset));
          }
        }
        // commit offsets to kafka clusters
        if (!partitionAndOffsetToCommit.isEmpty()) {
          try {
            Stopwatch stopwatch = scope.timer(MetricNames.OFFSET_COMMIT_LATENCY).start();
            KafkaConsumerTaskGroup taskGroup =
                rebalancingJobGroup.getJobGroup().getKafkaConsumerTaskGroup();
            AdminClient client = adminBuilder.build(taskGroup.getCluster());
            client.alterConsumerGroupOffsets(
                taskGroup.getConsumerGroup(), partitionAndOffsetToCommit);
            stopwatch.stop();
          } catch (Exception e) {
            // failed to commit offset might lead to wrong dlq lag reports.
            // we need to add metrics and alerts.
            logger.warn(
                MetricNames.OFFSET_COMMIT_FAILURE,
                StructuredLogging.kafkaCluster(
                    rebalancingJobGroup.getJobGroup().getKafkaConsumerTaskGroup().getCluster()),
                StructuredLogging.kafkaGroup(
                    rebalancingJobGroup
                        .getJobGroup()
                        .getKafkaConsumerTaskGroup()
                        .getConsumerGroup()),
                StructuredLogging.kafkaTopic(topic),
                e);
            scope
                .tagged(
                    StructuredTags.builder()
                        .setKafkaCluster(
                            rebalancingJobGroup
                                .getJobGroup()
                                .getKafkaConsumerTaskGroup()
                                .getCluster())
                        .setKafkaGroup(
                            rebalancingJobGroup
                                .getJobGroup()
                                .getKafkaConsumerTaskGroup()
                                .getConsumerGroup())
                        .setKafkaTopic(topic)
                        .build())
                .counter(MetricNames.OFFSET_COMMIT_FAILURE)
                .inc(1);
          }
        }
      }
    } finally {
      postProcessStopwatch.stop();
    }
  }

  private static class MetricNames {
    private static final String OFFSET_COMMIT_FAILURE = "offset.commit.failure";
    private static final String OFFSET_COMMIT_LATENCY = "controller.dlq.offset.commit.latency";
    private static final String POST_PROCESS_LATENCY = "controller.dlq.post.process.latency";

    private MetricNames() {}
  }
}
