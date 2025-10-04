package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/** Rebalancer is an interface that can be implemented for pluggable job assignment. */
public interface Rebalancer {

  /**
   * Compute new worker_id by updating the mutable {@link RebalancingJobGroup}.
   *
   * @param jobGroups that are registered to this data-transfer cluster.
   * @param workers is the set of live workers
   */
  default void computeWorkerId(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {
    if (workers.size() <= 0) {
      return;
    }
    final List<Long> workerIds = new ArrayList<>(workers.keySet());
    for (RebalancingJobGroup rebalancingJobGroup : jobGroups.values()) {
      for (Map.Entry<Long, StoredJob> jobEntry : rebalancingJobGroup.getJobs().entrySet()) {
        long jobId = jobEntry.getKey();
        long currentWorkerId = jobEntry.getValue().getWorkerId();
        if (!workerIds.contains(currentWorkerId)) {
          // only rebalance work if it is assigned to a stale worker.
          rebalancingJobGroup.updateJob(
              jobId,
              jobEntry.getValue().toBuilder()
                  .setWorkerId(
                      workerIds.get(ThreadLocalRandom.current().nextInt(0, workers.size())))
                  .build());
        }
      }
    }
  }

  /**
   * Compute new job configuration by updating the mutable {@link RebalancingJobGroup}.
   *
   * @param jobGroups that are registered to this data-transfer cluster.
   * @param workers is the set of live workers
   */
  default void computeJobConfiguration(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {
    for (RebalancingJobGroup rebalancingJobGroup : jobGroups.values()) {
      // manually read and divide job group quota by number of jobs.
      int numJobs = Math.max(rebalancingJobGroup.getJobs().size(), 1);
      double messagesPerSecPerJob =
          rebalancingJobGroup.getJobGroup().getFlowControl().getMessagesPerSec() / numJobs;
      double bytesPerSecPerJob =
          rebalancingJobGroup.getJobGroup().getFlowControl().getBytesPerSec() / numJobs;
      double maxInflightPerJob =
          rebalancingJobGroup.getJobGroup().getFlowControl().getMaxInflightMessages() / numJobs;
      double scalePerJob = rebalancingJobGroup.getScale().orElse(Scalar.ZERO) / numJobs;
      FlowControl flowControl =
          FlowControl.newBuilder()
              .setBytesPerSec(bytesPerSecPerJob)
              .setMessagesPerSec(messagesPerSecPerJob)
              .setMaxInflightMessages(maxInflightPerJob)
              .build();
      for (Map.Entry<Long, StoredJob> jobEntry : rebalancingJobGroup.getJobs().entrySet()) {
        rebalancingJobGroup.updateJob(
            jobEntry.getKey(),
            jobEntry.getValue().toBuilder()
                .setJob(
                    // use the job util that merges a new job group with the old job.
                    Rebalancer.mergeJobGroupAndJob(
                            rebalancingJobGroup.getJobGroup(), jobEntry.getValue().getJob())
                        .setFlowControl(flowControl) // override per partition flow control
                        .build())
                .setScale(scalePerJob)
                .build());
      }
    }
  }

  /**
   * Compute new job state by updating the mutable {@link RebalancingJobGroup}.
   *
   * @param jobGroups that are registered to this data-transfer cluster.
   * @param workers is the set of live workers
   */
  default void computeJobState(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {
    for (RebalancingJobGroup rebalancingJobGroup : jobGroups.values()) {
      JobState jobGroupState = rebalancingJobGroup.getJobGroupState();
      for (Map.Entry<Long, StoredJob> jobEntry : rebalancingJobGroup.getJobs().entrySet()) {
        JobState jobState = jobEntry.getValue().getState();
        if (!jobState.equals(jobGroupState)) {
          rebalancingJobGroup.updateJob(
              jobEntry.getKey(), jobEntry.getValue().toBuilder().setState(jobGroupState).build());
        }
      }
    }
  }

  /**
   * Performs some operation after all other operations. E.g., for jobs with the same start and end
   * offsets, the master needs to commit the offset to Kafka brokers.
   *
   * @param jobGroups that are registered to this data-transfer cluster.
   * @param workers is the set of live workers
   */
  default void postProcess(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers) {}

  /**
   * Before assign job to worker, compute load of reach jobGroup
   *
   * @param jobGroups the job groups
   */
  default void computeLoad(final Map<String, RebalancingJobGroup> jobGroups) {}

  /**
   * Returns a new job builder using the job group as the template and inferring fields from oldJob
   * where necessary.
   */
  static Job.Builder mergeJobGroupAndJob(JobGroup jobGroup, Job oldJob) {
    Job.Builder builder = Job.newBuilder();
    // The following fields are overridden from job group
    builder
        .getRpcDispatcherTaskBuilder()
        .setProcedure(jobGroup.getRpcDispatcherTaskGroup().getProcedure())
        .setRpcTimeoutMs(jobGroup.getRpcDispatcherTaskGroup().getRpcTimeoutMs())
        .setMaxRpcTimeouts(jobGroup.getRpcDispatcherTaskGroup().getMaxRpcTimeouts())
        .setRetryQueueTopic(jobGroup.getRpcDispatcherTaskGroup().getRetryQueueTopic())
        .setDlqTopic(jobGroup.getRpcDispatcherTaskGroup().getDlqTopic());
    builder
        .getKafkaConsumerTaskBuilder()
        .setProcessingDelayMs(jobGroup.getKafkaConsumerTaskGroup().getProcessingDelayMs());
    builder
        .getMiscConfigBuilder()
        .setOwnerServiceName(jobGroup.getMiscConfig().getOwnerServiceName())
        .setEnableDebug(jobGroup.getMiscConfig().getEnableDebug());
    builder.setExtension(jobGroup.getExtension());
    // service identities can be modified from the jobGroup without any impact to the pipeline
    // on the other hand isSecure flag changing would result in a new job group
    builder
        .getSecurityConfigBuilder()
        .addAllServiceIdentities(jobGroup.getSecurityConfig().getServiceIdentitiesList());
    builder.setResqConfig(jobGroup.getResqConfig());

    // The following fields retain the defaults from the old job
    builder.setType(oldJob.getType());
    builder.setJobId(oldJob.getJobId());
    builder.setFlowControl(oldJob.getFlowControl());
    builder.getSecurityConfigBuilder().setIsSecure(oldJob.getSecurityConfig().getIsSecure());
    // we defensively decide to keep the values for kafka consumer task from the old job
    // b/c changes in these configurations should generally result in a new job group b/c this is
    // the
    // stateful aspect of the pipeline (e.g., group/cluster/topic/partition/offsets/secure).
    builder
        .getKafkaConsumerTaskBuilder()
        .setCluster(oldJob.getKafkaConsumerTask().getCluster())
        .setTopic(oldJob.getKafkaConsumerTask().getTopic())
        .setPartition(oldJob.getKafkaConsumerTask().getPartition())
        .setConsumerGroup(oldJob.getKafkaConsumerTask().getConsumerGroup())
        .setAutoOffsetResetPolicy(oldJob.getKafkaConsumerTask().getAutoOffsetResetPolicy())
        .setIsolationLevel(oldJob.getKafkaConsumerTask().getIsolationLevel())
        .setStartOffset(oldJob.getKafkaConsumerTask().getStartOffset())
        .setEndOffset(oldJob.getKafkaConsumerTask().getEndOffset());
    builder
        .getRpcDispatcherTaskBuilder()
        .setUri(oldJob.getRpcDispatcherTask().getUri())
        .setRetryCluster(oldJob.getRpcDispatcherTask().getRetryCluster())
        .setDlqCluster(oldJob.getRpcDispatcherTask().getDlqCluster());
    builder
        .getRetryConfigBuilder()
        .setRetryEnabled(jobGroup.getRetryConfig().getRetryEnabled())
        .addAllRetryQueues(jobGroup.getRetryConfig().getRetryQueuesList());

    if (oldJob.getType().equals(JobType.JOB_TYPE_LOAD_GEN_PRODUCE)
        || oldJob.getType().equals(JobType.JOB_TYPE_KAFKA_REPLICATION)
        || oldJob.getType().equals(JobType.JOB_TYPE_KAFKA_AVAILABILITY)) {
      builder
          .getKafkaDispatcherTaskBuilder()
          .setCluster(oldJob.getKafkaDispatcherTask().getCluster())
          .setTopic(oldJob.getKafkaDispatcherTask().getTopic())
          .setDedupEnabled(oldJob.getKafkaDispatcherTask().getDedupEnabled())
          .setPartition(oldJob.getKafkaDispatcherTask().getPartition())
          .setIsSecure(oldJob.getKafkaDispatcherTask().getIsSecure())
          .setIsAcksOne(oldJob.getKafkaDispatcherTask().getIsAcksOne())
          .setEncodedFormatInfo(oldJob.getKafkaDispatcherTask().getEncodedFormatInfo());
    }

    if (oldJob.getType().equals(JobType.JOB_TYPE_KAFKA_AVAILABILITY)) {
      builder
          .getAvailabilityTaskBuilder()
          .setAvailabilityJobType(oldJob.getAvailabilityTask().getAvailabilityJobType())
          .setZoneIsolated(oldJob.getAvailabilityTask().getZoneIsolated());
    }

    if (oldJob.getType().equals(JobType.JOB_TYPE_KAFKA_AUDIT)) {
      builder
          .getAuditTaskBuilder()
          .setTopic(oldJob.getAuditTask().getTopic())
          .setCluster(oldJob.getAuditTask().getCluster())
          .setAuditMetadata(oldJob.getAuditTask().getAuditMetadata());
    }

    return builder;
  }
}
