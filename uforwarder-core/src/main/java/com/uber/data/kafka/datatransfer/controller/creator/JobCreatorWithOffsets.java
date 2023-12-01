package com.uber.data.kafka.datatransfer.controller.creator;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.JobUtils;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.data.kafka.datatransfer.common.TimestampUtils;
import com.uber.m3.tally.Scope;
import org.slf4j.Logger;

/** JobCreatorWithOffsets add a method to create jobs with start and end offsets */
public abstract class JobCreatorWithOffsets implements JobCreator {

  /**
   * Creates a StoredJob with start and end offsets. Meanwhile, logs and reports metrics.
   *
   * @param scope the scope used to report metrics
   * @param logger the logger used for logging
   * @param storedJobGroup the template
   * @param jobType either STREAMING or BATCH
   * @param jobId the jobId of the StoredJob
   * @param partition the partition of the StoredJob
   * @param startOffset the start offset for the StoredJob
   * @param endOffset the end offset for the StoredJob
   * @return a StoredJob with start and end offsets.
   */
  public StoredJob newJob(
      Scope scope,
      Logger logger,
      StoredJobGroup storedJobGroup,
      String jobType,
      long jobId,
      int partition,
      long startOffset,
      long endOffset) {
    logAndMetricHelper(scope, logger, storedJobGroup, jobType, jobId, partition);

    // we don't use JobUtils here because
    // 1. the current logic is clearer
    // 2. avoid creating many intermediate objects
    Job.Builder jobBuilder = Job.newBuilder(JobUtils.newJob(storedJobGroup.getJobGroup()));
    jobBuilder.setJobId(jobId);
    jobBuilder.getKafkaConsumerTaskBuilder().setPartition(partition);
    jobBuilder.getKafkaConsumerTaskBuilder().setStartOffset(startOffset).setEndOffset(endOffset);

    StoredJob.Builder storedJobBuilder = StoredJob.newBuilder();
    storedJobBuilder
        .setLastUpdated(TimestampUtils.currentTimeMilliseconds())
        // if startOffset >= endOffset, we don't need to assign the job, so we set it to canceled
        // state.
        .setState(
            startOffset >= endOffset ? JobState.JOB_STATE_CANCELED : storedJobGroup.getState())
        .setJob(jobBuilder.build());
    return storedJobBuilder.build();
  }

  private static void logAndMetricHelper(
      Scope scope,
      Logger logger,
      StoredJobGroup storedJobGroup,
      String jobType,
      long jobId,
      int partition) {
    KafkaConsumerTaskGroup kafkaConsumerGroupTask =
        storedJobGroup.getJobGroup().getKafkaConsumerTaskGroup();
    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaCluster(kafkaConsumerGroupTask.getCluster())
                .setKafkaGroup(kafkaConsumerGroupTask.getConsumerGroup())
                .setKafkaTopic(kafkaConsumerGroupTask.getTopic())
                .setJobType(jobType)
                .build())
        .counter("creator.job.create")
        .inc(1);
    logger.debug(
        "creator.job.create",
        StructuredLogging.jobId(jobId),
        StructuredLogging.kafkaCluster(kafkaConsumerGroupTask.getCluster()),
        StructuredLogging.kafkaGroup(kafkaConsumerGroupTask.getConsumerGroup()),
        StructuredLogging.kafkaTopic(kafkaConsumerGroupTask.getTopic()),
        StructuredLogging.kafkaPartition(partition),
        StructuredLogging.jobType(jobType));
  }
}
