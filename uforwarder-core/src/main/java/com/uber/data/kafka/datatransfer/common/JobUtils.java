package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.AuditMetaData;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobSnapshot;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import java.util.Collection;

/**
 * JobUtils for simplifying creating and updating Job proto entities.
 *
 * <p>The following naming conventions shall be used:
 *
 * <ol>
 *   <li>X getX() returns a field within an object.
 *   <li>X newX() creates anew object of type X. The input parameters of newX must not include the
 *       object X. To create a new object X from a parent prototype of the same type, use withX
 *       instead.
 *   <li>X withX() to create a new object with a single field set. This is useful for setting fields
 *       within an immutable object.
 *   <li>void setX() to set a field in a mutable object
 *   <li>boolean isX() for validation that returns boolean
 *   <li>void assertX() throws Exception for validations that throws exception
 * </ol>
 */
public final class JobUtils {
  /** UNSET_JOB_ID marks a job that has not yet been assigned a job id. */
  public static final long UNSET_JOB_ID = 0;
  /** UNSET_PARTITION marks a partition that is unset. */
  public static final int UNSET_PARTITION = -1;

  private JobUtils() {}

  /** Constructors must follow newXXX format * */

  /**
   * Creates a new JobSnapshot that merges the expected job and actual job status.
   *
   * @param job that contains the expected state of the job.
   * @param jobStatus contains the actual job status of the job.
   * @return JobSnapshot that aggregates expected job and actual job status.
   */
  public static JobSnapshot newJobSnapshot(StoredJob job, StoredJobStatus jobStatus) {
    return JobSnapshot.newBuilder().setExpectedJob(job).setActualJobStatus(jobStatus).build();
  }

  /** Create a new {@code StoredJob} with job set */
  /**
   * Creates a new Stored job given the provided input job.
   *
   * @param job to use as input for the stored job.
   * @return StoredJob that can be written to storage.
   */
  public static StoredJob newStoredJob(Job job) {
    StoredJob.Builder builder = StoredJob.newBuilder();
    builder.setJob(job);
    return setCurrentTimestamp(builder).build();
  }

  /** Returns a new Job.Builder with information from the JobGroup. */
  public static Job.Builder newJobBuilder(JobGroup jobGroup) {
    return newJobBuilder(
        jobGroup,
        UNSET_JOB_ID,
        UNSET_PARTITION,
        KafkaUtils.MAX_INVALID_START_OFFSET,
        KafkaUtils.MAX_INVALID_END_OFFSET,
        KafkaUtils.MAX_INVALID_START_OFFSET);
  }

  /**
   * Returns a new job builder using the job group as the template and setting the job specific
   * fields as specified.
   */
  public static Job.Builder newJobBuilder(
      JobGroup jobGroup,
      long jobId,
      int partition,
      long startOffset,
      long endOffset,
      long committedOffset) {
    Job.Builder builder = Job.newBuilder();
    builder.setJobId(jobId);
    builder.setType(jobGroup.getType());
    builder.setFlowControl(jobGroup.getFlowControl());
    builder.setSecurityConfig(jobGroup.getSecurityConfig());
    builder.setRetryConfig(jobGroup.getRetryConfig());
    builder.setResqConfig(jobGroup.getResqConfig());
    builder.setMiscConfig(jobGroup.getMiscConfig());
    builder
        .getKafkaConsumerTaskBuilder()
        .setCluster(jobGroup.getKafkaConsumerTaskGroup().getCluster())
        .setTopic(jobGroup.getKafkaConsumerTaskGroup().getTopic())
        .setPartition(partition)
        .setConsumerGroup(jobGroup.getKafkaConsumerTaskGroup().getConsumerGroup())
        .setAutoOffsetResetPolicy(jobGroup.getKafkaConsumerTaskGroup().getAutoOffsetResetPolicy())
        .setIsolationLevel(jobGroup.getKafkaConsumerTaskGroup().getIsolationLevel())
        .setStartOffset(startOffset)
        .setEndOffset(endOffset)
        .setCommittedOffset(committedOffset)
        .setProcessingDelayMs(jobGroup.getKafkaConsumerTaskGroup().getProcessingDelayMs())
        .setAuditMetadata(jobGroup.getKafkaConsumerTaskGroup().getAuditMetadata());
    if (JobType.JOB_TYPE_KAFKA_AVAILABILITY.equals(jobGroup.getType())) {
      builder
          .getKafkaDispatcherTaskBuilder()
          .setCluster(jobGroup.getKafkaDispatcherTaskGroup().getCluster())
          .setTopic(jobGroup.getKafkaDispatcherTaskGroup().getTopic())
          .setPartition(partition)
          .setDedupEnabled(jobGroup.getKafkaDispatcherTaskGroup().getDedupEnabled())
          .setIsSecure(jobGroup.getKafkaDispatcherTaskGroup().getIsSecure())
          .setIsAcksOne(jobGroup.getKafkaDispatcherTaskGroup().getIsAcksOne());
      builder
          .getAvailabilityTaskBuilder()
          .setAvailabilityJobType(jobGroup.getAvailabilityTaskGroup().getAvailabilityJobType())
          .setZoneIsolated(jobGroup.getAvailabilityTaskGroup().getZoneIsolated());
    }
    if (JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER.equals(jobGroup.getType())) {
      builder
          .getRpcDispatcherTaskBuilder()
          .setUri(jobGroup.getRpcDispatcherTaskGroup().getUri())
          .setProcedure(jobGroup.getRpcDispatcherTaskGroup().getProcedure())
          .setRpcTimeoutMs(jobGroup.getRpcDispatcherTaskGroup().getRpcTimeoutMs())
          .setMaxRpcTimeouts(jobGroup.getRpcDispatcherTaskGroup().getMaxRpcTimeouts())
          .setRetryQueueTopic(jobGroup.getRpcDispatcherTaskGroup().getRetryQueueTopic())
          .setRetryCluster(jobGroup.getRpcDispatcherTaskGroup().getRetryCluster())
          .setDlqTopic(jobGroup.getRpcDispatcherTaskGroup().getDlqTopic())
          .setDlqCluster(jobGroup.getRpcDispatcherTaskGroup().getDlqCluster());
    }
    return builder;
  }

  /**
   * creates a job from the jobGroup template
   *
   * @param jobGroup the jobGroup template
   * @return the job created from the jobGroup template
   */
  public static Job newJob(JobGroup jobGroup) {
    return newJobBuilder(jobGroup).build();
  }

  /**
   * Gets the job key for the provided job.
   *
   * <p>Note: job_key is a partition key for a job that is unique within a job group. But it is not
   * necessarily unique across all jobs. For globally unique job id, use job_id.
   *
   * @param job to query the job key for.
   * @return the job key for this job.
   * @throws UnsupportedOperationException if job key is nto well defined for this job.
   */
  public static int getJobKey(StoredJob job) throws UnsupportedOperationException {
    JobType type = job.getJob().getType();
    switch (type) {
      case JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER:
      case JOB_TYPE_KAFKA_AUDIT:
      case JOB_TYPE_KAFKA_REPLICATION:
      case JOB_TYPE_LOAD_GEN_CONSUME:
        return job.getJob().getKafkaConsumerTask().getPartition();
      case JOB_TYPE_LOAD_GEN_PRODUCE:
        return job.getJob().getKafkaDispatcherTask().getPartition();
      default:
        throw new UnsupportedOperationException(type.toString());
    }
  }

  /** Setters for immutable objects must follow withXXX format * */

  /** Returns a new {@code StoredJobGroup} with storedJobId set. */
  /**
   * Sets the job group id for this job group.
   *
   * @param jobGroupId to set.
   * @param jobGroup to set the job group id into.
   * @return new StoredJobGroup with id set.
   * @implNote the parameter order is reversed from the standard withX(object, toSet) pattern
   *     because this is enforced by the BiFunction<K, V, V> creator interface.
   */
  public static StoredJobGroup withJobGroupId(String jobGroupId, StoredJobGroup jobGroup) {
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder(jobGroup);
    builder.getJobGroupBuilder().setJobGroupId(jobGroupId);
    return setCurrentTimestamp(builder).build();
  }

  /**
   * Sets the job key for this job.
   *
   * @param job to set job key within.
   * @param jobKey to set.
   * @return a new Job with job key modified.
   */
  public static Job withJobKey(Job job, int jobKey) {
    return withJobKey(Job.newBuilder(job), jobKey);
  }

  /**
   * Sets the job key for this job builder.
   *
   * @param builder to set job key within.
   * @param jobKey to set.
   * @return a new Job with job key modified.
   */
  public static Job withJobKey(Job.Builder builder, int jobKey) {
    switch (builder.getType()) {
      case JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER:
      case JOB_TYPE_KAFKA_REPLICATION:
      case JOB_TYPE_KAFKA_AUDIT:
      case JOB_TYPE_LOAD_GEN_CONSUME:
        builder.getKafkaConsumerTaskBuilder().setPartition(jobKey);
        break;
      case JOB_TYPE_LOAD_GEN_PRODUCE:
        builder.getKafkaDispatcherTaskBuilder().setPartition(jobKey);
        break;
    }
    return builder.build();
  }

  /**
   * Sets workerId for this job snapshot by creating a new immutable JobSnapshot.
   *
   * @param jobSnapshot job snapshot.
   * @param workerId to set.
   * @return new JobSnapshot with workerId set.
   */
  public static JobSnapshot withWorkerId(JobSnapshot jobSnapshot, long workerId) {
    JobSnapshot.Builder builder = JobSnapshot.newBuilder(jobSnapshot);
    builder.getExpectedJobBuilder().setWorkerId(workerId);
    return builder.build();
  }

  /**
   * Sets the job list for this job group.
   *
   * @param jobGroup to set job list for.
   * @param jobsList to set.
   * @return StoredJobGroup with job list set.
   */
  public static StoredJobGroup withJobsList(StoredJobGroup jobGroup, Iterable<StoredJob> jobsList) {
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder(jobGroup);
    builder.clearJobs();
    builder.addAllJobs(jobsList);
    return setCurrentTimestamp(builder).build();
  }

  /**
   * Sets the job group state for this job group.
   *
   * @param jobGroup to set job group state for.
   * @param jobState to set.
   * @return StoredJobGroup with the new job group state.
   */
  public static StoredJobGroup withJobGroupState(StoredJobGroup jobGroup, JobState jobState) {
    return StoredJobGroup.newBuilder(jobGroup).setState(jobState).build();
  }

  /**
   * Checks if the job is an unpartitioned job.
   *
   * <p>This is useful for validating that a job represents the aggregation of a group of
   * partitioned jobs.
   *
   * @param job to validate.
   * @throws IllegalArgumentException if it fails validation.
   */
  public static void assertUnpartitionedJob(Job job) throws IllegalArgumentException {
    if (job.getJobId() != UNSET_JOB_ID) {
      throw new IllegalArgumentException(String.format("expected job_id == %d", UNSET_JOB_ID));
    }
    switch (job.getType()) {
      case JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER:
        if (job.getKafkaConsumerTask().getPartition() != UNSET_PARTITION) {
          throw new IllegalArgumentException(
              String.format("expected KafkaConsumerTask == %d", UNSET_PARTITION));
        }
        break;
    }
  }

  /** Setters for for mutable protobuf builders */

  /**
   * Set the last update timestamp to the current time.
   *
   * @param builder to set last update timestamp for.
   * @return builder with timestamp set.
   */
  public static StoredJob.Builder setCurrentTimestamp(StoredJob.Builder builder) {
    builder.setLastUpdated(TimestampUtils.currentTimeMilliseconds());
    return builder;
  }

  /**
   * Set the last update timestamp to the current time.
   *
   * @param builder to set last update timestamp for.
   * @return builder with timestamp set.
   */
  public static StoredJobGroup.Builder setCurrentTimestamp(StoredJobGroup.Builder builder) {
    builder.setLastUpdated(TimestampUtils.currentTimeMilliseconds());
    return builder;
  }

  /**
   * Set the last update timestamp to the current time.
   *
   * @param builder to set last update timestamp for.
   * @return builder with timestamp set.
   */
  public static StoredJobStatus.Builder setCurrentTimestamp(StoredJobStatus.Builder builder) {
    builder.setLastUpdated(TimestampUtils.currentTimeMilliseconds());
    return builder;
  }

  /** compares whether two job configurations are the same except for start offset */
  public static boolean isSameExceptStartOffset(Job job1, Job job2) {
    return clearStartOffset(job1).equals(clearStartOffset(job2));
  }

  /** Resets the start offset to zero * */
  private static Job clearStartOffset(Job job) {
    Job.Builder builder = Job.newBuilder(job);
    builder.getKafkaConsumerTaskBuilder().clearStartOffset();
    return builder.build();
  }

  /** Returns true if the provided job is derived from the job group, otherwise false. */
  public static boolean isDerived(JobGroup jobGroup, Job job) {
    Job jobGroupDerivedJob = newJob(jobGroup);
    Job.Builder derivedJobBuilder = Job.newBuilder(job);
    derivedJobBuilder.setJobId(UNSET_JOB_ID);
    derivedJobBuilder
        .getKafkaConsumerTaskBuilder()
        .setPartition(UNSET_PARTITION)
        .setStartOffset(KafkaUtils.MAX_INVALID_START_OFFSET)
        .setEndOffset(KafkaUtils.MAX_INVALID_END_OFFSET)
        .setCommittedOffset(KafkaUtils.MAX_INVALID_START_OFFSET)
        .setAuditMetadata(AuditMetaData.newBuilder().build());
    return jobGroupDerivedJob.equals(derivedJobBuilder.build());
  }

  /**
   * Checks whether a StoredJobGroup (including the grouped job and the list of jobs) is canceled or
   * not.
   *
   * @param jobGroup The StoredJobGroup to check.
   * @return true if the StoredJobGroup is canceled; false if not.
   */
  public static boolean isJobGroupCanceled(StoredJobGroup jobGroup) {
    if (jobGroup.getState() != JobState.JOB_STATE_CANCELED) {
      return false;
    }
    return areJobsAllCanceled(jobGroup.getJobsList());
  }

  /**
   * Checks whether a list of jobs are all canceled or not.
   *
   * @param jobCollection The collection of jobs to check
   * @return true if the collection of jobs are all canceled; false if not.
   */
  public static boolean areJobsAllCanceled(Collection<StoredJob> jobCollection) {
    for (StoredJob storedJob : jobCollection) {
      if (storedJob.getState() != JobState.JOB_STATE_CANCELED) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks whether a list of jobs are (1) not empty (2) all canceled.
   *
   * @param jobCollection The collection of jobs to check
   * @return true if the collection is not empty and all jobs are all canceled; false if not.
   */
  public static boolean areJobsNotEmptyAndAllCanceled(Collection<StoredJob> jobCollection) {
    if (jobCollection.isEmpty()) {
      return false;
    }
    return areJobsAllCanceled(jobCollection);
  }

  /**
   * gets the cluster to produce Kafka messages to.
   *
   * @param job the job to extract the cluster to produce kafka messages to.
   * @return the cluster to produce Kafka messages to.
   */
  public static String getKafkaProducerCluster(Job job) {
    switch (job.getType()) {
      case JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER:
        return "dlq";
      case JOB_TYPE_KAFKA_AVAILABILITY:
      case JOB_TYPE_KAFKA_REPLICATION:
      case JOB_TYPE_LOAD_GEN_PRODUCE:
        return job.getKafkaDispatcherTask().getCluster();
      default:
        // should get from other configure field.
        // default to empty cluster name.
        return "";
    }
  }
}
