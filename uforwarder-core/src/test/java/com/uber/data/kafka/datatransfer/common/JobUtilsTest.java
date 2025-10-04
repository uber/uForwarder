package com.uber.data.kafka.datatransfer.common;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.uber.data.kafka.datatransfer.AuditConfig;
import com.uber.data.kafka.datatransfer.AuditMetaData;
import com.uber.data.kafka.datatransfer.AuditType;
import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.AvailabilityJobType;
import com.uber.data.kafka.datatransfer.AvailabilityTask;
import com.uber.data.kafka.datatransfer.AvailabilityTaskGroup;
import com.uber.data.kafka.datatransfer.EncodedFormatInfo;
import com.uber.data.kafka.datatransfer.EncodedFormatType;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.KafkaDispatcherTask;
import com.uber.data.kafka.datatransfer.KafkaDispatcherTaskGroup;
import com.uber.data.kafka.datatransfer.MiscConfig;
import com.uber.data.kafka.datatransfer.ResqConfig;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.data.kafka.datatransfer.RpcDispatcherTaskGroup;
import com.uber.data.kafka.datatransfer.SecurityConfig;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JobUtilsTest {

  private StoredJob job;
  private StoredJob rpcJob;
  private StoredJobStatus jobStatus;
  private StoredJobGroup storedJobGroup;

  @BeforeEach
  public void setup() {
    StoredJob.Builder builder = StoredJob.newBuilder();
    builder.getJobBuilder().setJobId(1);
    job = builder.build();

    builder.getJobBuilder().setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER);
    builder.getJobBuilder().setJobId(2);
    rpcJob = builder.build();

    StoredJobStatus.Builder jobStatusBuilder = StoredJobStatus.newBuilder();
    jobStatusBuilder.getJobStatusBuilder().getJobBuilder().setJobId(1);
    jobStatus = jobStatusBuilder.build();

    StoredJobGroup.Builder storeJobGroupBuilder = StoredJobGroup.newBuilder();
    storeJobGroupBuilder.setJobGroup(JobGroup.newBuilder().build());
    storedJobGroup = storeJobGroupBuilder.build();
  }

  @Test
  public void newJobSnapshot() {
    Assertions.assertEquals(
        1, JobUtils.newJobSnapshot(job, jobStatus).getExpectedJob().getJob().getJobId());
  }

  @Test
  public void newStoredJob() {
    Assertions.assertEquals(1, JobUtils.newStoredJob(job.getJob()).getJob().getJobId());
  }

  @Test
  public void getJobKeyForConsumerToRpcDispatcherJob() {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder(job);
    jobBuilder.getJobBuilder().setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER);
    Assertions.assertEquals(0, JobUtils.getJobKey(jobBuilder.build()));
  }

  @Test
  public void getJobKeyForAuditJob() {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder(job);
    jobBuilder.getJobBuilder().setType(JobType.JOB_TYPE_KAFKA_AUDIT);
    Assertions.assertEquals(0, JobUtils.getJobKey(jobBuilder.build()));
  }

  @Test
  public void getUnsupportedJobKey() {
    assertThrows(UnsupportedOperationException.class, () -> JobUtils.getJobKey(job));
  }

  @Test
  public void withJobKeyForConsumerToRpcDispatcherJob() {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder(job);

    jobBuilder.getJobBuilder().setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER);
    Assertions.assertEquals(
        2,
        JobUtils.getJobKey(
            StoredJob.newBuilder()
                .setJob(JobUtils.withJobKey(jobBuilder.build().getJob(), 2))
                .build()));
  }

  @Test
  public void withJobKeyForAuditJob() {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder(job);

    jobBuilder.getJobBuilder().setType(JobType.JOB_TYPE_KAFKA_AUDIT);
    Assertions.assertEquals(
        2,
        JobUtils.getJobKey(
            StoredJob.newBuilder()
                .setJob(JobUtils.withJobKey(jobBuilder.build().getJob(), 2))
                .build()));
  }

  @Test
  public void withJobsList() {
    List<StoredJob> jobsList = ImmutableList.of(job);
    Assertions.assertEquals(
        jobsList, JobUtils.withJobsList(storedJobGroup, jobsList).getJobsList());
  }

  @Test
  public void withJobGroupState() {
    Assertions.assertEquals(
        JobState.JOB_STATE_RUNNING,
        JobUtils.withJobGroupState(storedJobGroup, JobState.JOB_STATE_RUNNING).getState());
  }

  @Test
  public void assertUnpartitionedJob() throws Exception {
    Job.Builder builder = Job.newBuilder(rpcJob.getJob());
    builder.setJobId(0);
    Job job = JobUtils.withJobKey(builder.build(), -1);
    JobUtils.assertUnpartitionedJob(job);
  }

  @Test
  public void assertUnpartitionedJobWithJobId() {
    assertThrows(
        IllegalArgumentException.class, () -> JobUtils.assertUnpartitionedJob(rpcJob.getJob()));
  }

  @Test
  public void assertUnpartitionedJobWithJobKey() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Job.Builder builder = Job.newBuilder(rpcJob.getJob());
          builder.setJobId(0);
          JobUtils.assertUnpartitionedJob(builder.build());
        });
  }

  @Test
  public void setCurrentTimestamp() {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    JobUtils.setCurrentTimestamp(jobBuilder);
    Assertions.assertNotEquals(0, jobBuilder.build().getLastUpdated().getSeconds());

    StoredJobGroup.Builder groupBuilder = StoredJobGroup.newBuilder();
    JobUtils.setCurrentTimestamp(groupBuilder);
    Assertions.assertNotEquals(0, groupBuilder.build().getLastUpdated().getSeconds());

    StoredJobStatus.Builder statusBuilder = StoredJobStatus.newBuilder();
    JobUtils.setCurrentTimestamp(statusBuilder);
    Assertions.assertNotEquals(0, statusBuilder.build().getLastUpdated().getSeconds());
  }

  @Test
  public void isSameExceptStartOffset() {
    Job.Builder builder = Job.newBuilder();
    builder.getKafkaConsumerTaskBuilder().setEndOffset(100);

    builder.getKafkaConsumerTaskBuilder().setStartOffset(10);
    Job job1 = builder.build();

    builder.getKafkaConsumerTaskBuilder().setStartOffset(20);
    Job job2 = builder.build();

    Assertions.assertTrue(JobUtils.isSameExceptStartOffset(job1, job2));
  }

  @Test
  public void isJobGroupCanceled() {
    StoredJobGroup.Builder builder = StoredJobGroup.newBuilder();
    Assertions.assertFalse(JobUtils.isJobGroupCanceled(builder.build()));

    builder.setState(JobState.JOB_STATE_CANCELED);
    Assertions.assertTrue(JobUtils.isJobGroupCanceled(builder.build()));
  }

  @Test
  public void areJobsAllCanceled() {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    StoredJobGroup.Builder groupBuilder = StoredJobGroup.newBuilder();

    Assertions.assertTrue(JobUtils.areJobsAllCanceled(groupBuilder.build().getJobsList()));

    jobBuilder.setState(JobState.JOB_STATE_CANCELED);
    groupBuilder.addJobs(jobBuilder.build());
    Assertions.assertTrue(JobUtils.areJobsAllCanceled(groupBuilder.build().getJobsList()));

    jobBuilder.setState(JobState.JOB_STATE_RUNNING);
    groupBuilder.addJobs(jobBuilder.build());
    Assertions.assertFalse(JobUtils.areJobsAllCanceled(groupBuilder.build().getJobsList()));
  }

  @Test
  public void areJobsNotEmptyAndAllCanceled() {
    StoredJob.Builder jobBuilder = StoredJob.newBuilder();
    StoredJobGroup.Builder groupBuilder = StoredJobGroup.newBuilder();

    Assertions.assertFalse(
        JobUtils.areJobsNotEmptyAndAllCanceled(groupBuilder.build().getJobsList()));

    jobBuilder.setState(JobState.JOB_STATE_CANCELED);
    groupBuilder.addJobs(jobBuilder.build());
    Assertions.assertTrue(
        JobUtils.areJobsNotEmptyAndAllCanceled(groupBuilder.build().getJobsList()));

    jobBuilder.setState(JobState.JOB_STATE_RUNNING);
    groupBuilder.addJobs(jobBuilder.build());
    Assertions.assertFalse(
        JobUtils.areJobsNotEmptyAndAllCanceled(groupBuilder.build().getJobsList()));
  }

  @Test
  public void getKafkaProducerCluster() {
    Job.Builder builder = Job.newBuilder();
    Assertions.assertEquals("", JobUtils.getKafkaProducerCluster(builder.build()));

    builder.setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER);
    Assertions.assertEquals("dlq", JobUtils.getKafkaProducerCluster(builder.build()));

    builder.setType(JobType.JOB_TYPE_KAFKA_REPLICATION);
    builder.setKafkaDispatcherTask(
        KafkaDispatcherTask.newBuilder().setCluster("kloak-dca1a").build());
    Assertions.assertEquals("kloak-dca1a", JobUtils.getKafkaProducerCluster(builder.build()));

    builder.setType(JobType.JOB_TYPE_KAFKA_AVAILABILITY);
    builder.setKafkaDispatcherTask(
        KafkaDispatcherTask.newBuilder().setCluster("kloak-phx2d").build());
    Assertions.assertEquals("kloak-phx2d", JobUtils.getKafkaProducerCluster(builder.build()));
  }

  @Test
  public void testNewJobForConsumerToRpcDispatcherJob() {
    String cluster = "test-cluster";
    String consumeGroup = "test-consumer-group";
    String topic = "test-topic";
    String uri = "test-uri";
    String procedure = "test-procedure";
    int rpcTimeoutMs = 1000;
    int maxRpcTimeouts = 3;
    String retryQueueTopic = "test-retry-topic";
    String retryCluster = "test-retry-cluster";
    String dlqTopic = "test-dlq-topic";
    String dlqCluster = "test-dlq-cluster";
    String jobGroupId = "test-job-group";
    boolean isSecure = true;
    List<String> serviceIdentities = Arrays.asList("id1", "id2");
    int processingDelayMs = 2000;
    int maxRetryCount = 4;
    boolean retryEnabled = true;
    List<RetryQueue> retryQueues =
        Arrays.asList(
            RetryQueue.newBuilder()
                .setRetryQueueTopic(retryQueueTopic)
                .setRetryCluster(retryCluster)
                .setProcessingDelayMs(processingDelayMs)
                .setMaxRetryCount(maxRetryCount)
                .build());
    KafkaConsumerTaskGroup kafkaConsumerTaskGroup =
        JobGroup.newBuilder()
            .getKafkaConsumerTaskGroupBuilder()
            .setCluster(cluster)
            .setConsumerGroup(consumeGroup)
            .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST)
            .setTopic(topic)
            .build();
    RpcDispatcherTaskGroup rpcDispatcherTaskGroup =
        JobGroup.newBuilder()
            .getRpcDispatcherTaskGroupBuilder()
            .setUri(uri)
            .setProcedure(procedure)
            .setRpcTimeoutMs(rpcTimeoutMs)
            .setMaxRpcTimeouts(maxRpcTimeouts)
            .setRetryQueueTopic(retryQueueTopic)
            .setRetryCluster(retryCluster)
            .setDlqTopic(dlqTopic)
            .setDlqCluster(dlqCluster)
            .build();
    SecurityConfig securityConfig =
        JobGroup.newBuilder()
            .getSecurityConfigBuilder()
            .setIsSecure(isSecure)
            .addAllServiceIdentities(serviceIdentities)
            .build();
    RetryConfig retryConfig =
        RetryConfig.newBuilder()
            .setRetryEnabled(retryEnabled)
            .addAllRetryQueues(retryQueues)
            .build();
    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setJobGroupId(jobGroupId)
            .setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER)
            .setKafkaConsumerTaskGroup(kafkaConsumerTaskGroup)
            .setRpcDispatcherTaskGroup(rpcDispatcherTaskGroup)
            .setSecurityConfig(securityConfig)
            .setRetryConfig(retryConfig)
            .build();
    Job job = JobUtils.newJob(jobGroup);
    Assertions.assertEquals(0, job.getJobId());
    Assertions.assertEquals(-1, job.getKafkaConsumerTask().getPartition());
    Assertions.assertEquals(-1, job.getKafkaConsumerTask().getStartOffset());
    Assertions.assertEquals(0, job.getKafkaConsumerTask().getEndOffset());
    Assertions.assertEquals(cluster, job.getKafkaConsumerTask().getCluster());
    Assertions.assertEquals(consumeGroup, job.getKafkaConsumerTask().getConsumerGroup());
    Assertions.assertEquals(
        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST,
        job.getKafkaConsumerTask().getAutoOffsetResetPolicy());
    Assertions.assertEquals(topic, job.getKafkaConsumerTask().getTopic());
    Assertions.assertEquals(uri, job.getRpcDispatcherTask().getUri());
    Assertions.assertEquals(procedure, job.getRpcDispatcherTask().getProcedure());
    Assertions.assertEquals(rpcTimeoutMs, job.getRpcDispatcherTask().getRpcTimeoutMs());
    Assertions.assertEquals(maxRpcTimeouts, job.getRpcDispatcherTask().getMaxRpcTimeouts());
    Assertions.assertEquals(retryQueueTopic, job.getRpcDispatcherTask().getRetryQueueTopic());
    Assertions.assertEquals(retryCluster, job.getRpcDispatcherTask().getRetryCluster());
    Assertions.assertEquals(dlqTopic, job.getRpcDispatcherTask().getDlqTopic());
    Assertions.assertEquals(dlqCluster, job.getRpcDispatcherTask().getDlqCluster());
    Assertions.assertEquals(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER, job.getType());
    Assertions.assertEquals(
        0, job.getKafkaConsumerTask().getAuditMetadata().getAuditConfigsList().size());
    Assertions.assertEquals(isSecure, job.getSecurityConfig().getIsSecure());
    Assertions.assertEquals(serviceIdentities, job.getSecurityConfig().getServiceIdentitiesList());
    Assertions.assertEquals(retryEnabled, job.getRetryConfig().getRetryEnabled());
    Assertions.assertEquals(retryQueues, job.getRetryConfig().getRetryQueuesList());
  }

  @Test
  public void testNewJobForAuditJob() {
    String cluster = "test-cluster";
    String consumeGroup = "test-consumer-group";
    String topic = "test-topic";
    int auditIntervalInSecs = 600;
    String jobGroupId = "test-job-group";
    KafkaConsumerTaskGroup kafkaConsumerTaskGroup =
        JobGroup.newBuilder()
            .getKafkaConsumerTaskGroupBuilder()
            .setCluster(cluster)
            .setConsumerGroup(consumeGroup)
            .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST)
            .setTopic(topic)
            .setAuditMetadata(
                AuditMetaData.newBuilder()
                    .addAuditConfigs(
                        AuditConfig.newBuilder()
                            .setAuditType(AuditType.AUDIT_TYPE_EXACT_UNIQ)
                            .setAuditIntervalInSeconds(auditIntervalInSecs)))
            .build();
    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setJobGroupId(jobGroupId)
            .setType(JobType.JOB_TYPE_KAFKA_AUDIT)
            .setKafkaConsumerTaskGroup(kafkaConsumerTaskGroup)
            .build();
    Job job = JobUtils.newJob(jobGroup);
    Assertions.assertEquals(0, job.getJobId());
    Assertions.assertEquals(-1, job.getKafkaConsumerTask().getPartition());
    Assertions.assertEquals(-1, job.getKafkaConsumerTask().getStartOffset());
    Assertions.assertEquals(0, job.getKafkaConsumerTask().getEndOffset());
    Assertions.assertEquals(cluster, job.getKafkaConsumerTask().getCluster());
    Assertions.assertEquals(consumeGroup, job.getKafkaConsumerTask().getConsumerGroup());
    Assertions.assertEquals(
        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST,
        job.getKafkaConsumerTask().getAutoOffsetResetPolicy());
    Assertions.assertEquals(topic, job.getKafkaConsumerTask().getTopic());
    Assertions.assertEquals(JobType.JOB_TYPE_KAFKA_AUDIT, job.getType());
    Assertions.assertEquals(
        AuditType.AUDIT_TYPE_EXACT_UNIQ,
        job.getKafkaConsumerTask().getAuditMetadata().getAuditConfigs(0).getAuditType());
    Assertions.assertEquals(
        auditIntervalInSecs,
        job.getKafkaConsumerTask()
            .getAuditMetadata()
            .getAuditConfigs(0)
            .getAuditIntervalInSeconds());
  }

  @Test
  public void testIsDerivedForConsumerToRpcDispatcherJob() {
    JobGroup.Builder jobGroupBuilder =
        JobGroup.newBuilder().setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER);
    Job.Builder jobBuilder =
        Job.newBuilder()
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().build())
            .setFlowControl(FlowControl.newBuilder().build())
            .setSecurityConfig(SecurityConfig.newBuilder().build())
            .setRetryConfig(RetryConfig.newBuilder().build())
            .setResqConfig(ResqConfig.newBuilder().build())
            .setMiscConfig(MiscConfig.newBuilder().build())
            .setExtension(Any.getDefaultInstance())
            .setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER);

    Assertions.assertTrue(JobUtils.isDerived(jobGroupBuilder.build(), jobBuilder.build()));

    jobBuilder.getKafkaConsumerTaskBuilder().setPartition(1);
    Assertions.assertTrue(JobUtils.isDerived(jobGroupBuilder.build(), jobBuilder.build()));

    jobGroupBuilder.getKafkaConsumerTaskGroupBuilder().setCluster("cluster");
    Assertions.assertFalse(JobUtils.isDerived(jobGroupBuilder.build(), jobBuilder.build()));
  }

  @Test
  public void testIsDerivedForAuditJob() {
    JobGroup.Builder jobGroupBuilder = JobGroup.newBuilder().setType(JobType.JOB_TYPE_KAFKA_AUDIT);
    Job.Builder jobBuilder =
        Job.newBuilder()
            .setFlowControl(FlowControl.newBuilder().build())
            .setSecurityConfig(SecurityConfig.newBuilder().build())
            .setRetryConfig(RetryConfig.newBuilder().build())
            .setResqConfig(ResqConfig.newBuilder().build())
            .setMiscConfig(MiscConfig.newBuilder().build())
            .setExtension(Any.getDefaultInstance())
            .setType(JobType.JOB_TYPE_KAFKA_AUDIT);

    Assertions.assertTrue(JobUtils.isDerived(jobGroupBuilder.build(), jobBuilder.build()));

    jobBuilder.getKafkaConsumerTaskBuilder().setPartition(1);
    Assertions.assertTrue(JobUtils.isDerived(jobGroupBuilder.build(), jobBuilder.build()));

    jobGroupBuilder.getKafkaConsumerTaskGroupBuilder().setCluster("cluster");
    Assertions.assertFalse(JobUtils.isDerived(jobGroupBuilder.build(), jobBuilder.build()));
  }

  @Test
  public void testResqConfigsPropagatedToJobFromJobGroup() {
    String resqCluster = "resq-cluster";
    String resqTopic = "resq-topic";
    FlowControl flowControl =
        FlowControl.newBuilder()
            .setBytesPerSec(1000)
            .setMessagesPerSec(10)
            .setMaxInflightMessages(2)
            .build();
    JobGroup.Builder jobGroupBuilder =
        JobGroup.newBuilder().setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER);
    // Created job should have empty resilience queue configs
    Assertions.assertEquals(
        ResqConfig.newBuilder().build(),
        JobUtils.newJobBuilder(jobGroupBuilder.build()).build().getResqConfig());
    ResqConfig resqConfig =
        ResqConfig.newBuilder()
            .setResqCluster(resqCluster)
            .setResqTopic(resqTopic)
            .setResqEnabled(true)
            .setFlowControl(flowControl)
            .build();
    jobGroupBuilder.setResqConfig(resqConfig);
    // Created job should have resilience queue configs same as job group
    Assertions.assertEquals(
        resqConfig, JobUtils.newJobBuilder(jobGroupBuilder.build()).build().getResqConfig());
  }

  @Test
  public void testExtensionPropagatedToJobFromJobGroup() throws InvalidProtocolBufferException {
    JobGroup.Builder jobGroupBuilder =
        JobGroup.newBuilder()
            .setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER)
            .setExtension(Any.pack(MiscConfig.newBuilder().setEnableDebug(true).build()));
    Job.Builder jobBuilder = JobUtils.newJobBuilder(jobGroupBuilder.build());
    Assertions.assertTrue(
        jobBuilder.build().getExtension().unpack(MiscConfig.class).getEnableDebug());
  }

  @Test
  public void testKafkaDispatcherTaskPropagatedToJobFromJobGroup() {
    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setType(JobType.JOB_TYPE_KAFKA_AVAILABILITY)
            .setKafkaDispatcherTaskGroup(
                KafkaDispatcherTaskGroup.newBuilder()
                    .setCluster("cluster")
                    .setTopic("topic")
                    .setDedupEnabled(true)
                    .setIsSecure(false)
                    .setIsAcksOne(true)
                    .setEncodedFormatInfo(
                        EncodedFormatInfo.newBuilder()
                            .setEncodedFormatType(EncodedFormatType.ENCODED_FORMAT_TYPE_PROTOBUF)
                            .setSchemaVersion(1)
                            .build())
                    .build())
            .build();

    KafkaDispatcherTask kafkaDispatcherTask =
        KafkaDispatcherTask.newBuilder()
            .setCluster("cluster")
            .setTopic("topic")
            .setPartition(-1)
            .setDedupEnabled(true)
            .setIsSecure(false)
            .setIsAcksOne(true)
            .setEncodedFormatInfo(
                EncodedFormatInfo.newBuilder()
                    .setSchemaVersion(1)
                    .setEncodedFormatType(EncodedFormatType.ENCODED_FORMAT_TYPE_PROTOBUF)
                    .build())
            .build();

    Assertions.assertEquals(
        kafkaDispatcherTask, JobUtils.newJobBuilder(jobGroup).build().getKafkaDispatcherTask());
  }

  @Test
  public void testAvailabilityTaskPropagatedToJobFromJobGroup() {
    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setType(JobType.JOB_TYPE_KAFKA_AVAILABILITY)
            .setAvailabilityTaskGroup(
                AvailabilityTaskGroup.newBuilder()
                    .setAvailabilityJobType(
                        AvailabilityJobType.AVAILABILITY_JOB_TYPE_NATIVE_PRODUCER)
                    .setZoneIsolated(true)
                    .build())
            .build();

    AvailabilityTask availabilityTask =
        AvailabilityTask.newBuilder()
            .setAvailabilityJobType(AvailabilityJobType.AVAILABILITY_JOB_TYPE_NATIVE_PRODUCER)
            .setZoneIsolated(true)
            .build();

    Assertions.assertEquals(
        availabilityTask, JobUtils.newJobBuilder(jobGroup).build().getAvailabilityTask());
  }

  @Test
  public void testMiscConfigPropagatedToJobFromJobGroup() {
    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER)
            .setMiscConfig(MiscConfig.newBuilder().setOwnerServiceName("owner-service").build())
            .build();
    Job.Builder jobBuilder = JobUtils.newJobBuilder(jobGroup);
    Assertions.assertNotNull(jobBuilder.getMiscConfig());
    Assertions.assertEquals("owner-service", jobBuilder.getMiscConfig().getOwnerServiceName());
  }
}
