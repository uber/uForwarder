package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Any;
import com.google.protobuf.util.Timestamps;
import com.uber.data.kafka.datatransfer.AuditConfig;
import com.uber.data.kafka.datatransfer.AuditMetaData;
import com.uber.data.kafka.datatransfer.AuditTask;
import com.uber.data.kafka.datatransfer.AuditTaskGroup;
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
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.KafkaDispatcherTask;
import com.uber.data.kafka.datatransfer.KafkaDispatcherTaskGroup;
import com.uber.data.kafka.datatransfer.MiscConfig;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.ResqConfig;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.data.kafka.datatransfer.ScaleStatus;
import com.uber.data.kafka.datatransfer.SecurityConfig;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RebalancerTest extends FievelTestBase {
  private Rebalancer rebalancer;
  private Map<String, RebalancingJobGroup> jobGroupMap;
  private Map<Long, StoredWorker> workerMap;

  @Before
  public void setup() {
    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.setState(JobState.JOB_STATE_RUNNING);
    jobGroupBuilder.setScaleStatus(ScaleStatus.newBuilder().setScale(2.0).build());
    jobGroupBuilder.setJobGroup(
        JobGroup.newBuilder()
            .setFlowControl(
                FlowControl.newBuilder()
                    .setMessagesPerSec(4)
                    .setBytesPerSec(8)
                    .setMaxInflightMessages(2)
                    .build())
            .build());
    jobGroupBuilder.addJobs(
        StoredJob.newBuilder()
            .setWorkerId(1)
            .setState(JobState.JOB_STATE_RUNNING)
            .setJob(Job.newBuilder().setJobId(1).build())
            .build());
    jobGroupBuilder.addJobs(
        StoredJob.newBuilder()
            .setWorkerId(2)
            .setState(JobState.JOB_STATE_INVALID)
            .setJob(Job.newBuilder().setJobId(2).build())
            .build());
    jobGroupMap =
        ImmutableMap.of(
            "test_topic",
            RebalancingJobGroup.of(Versioned.from(jobGroupBuilder.build(), 1), ImmutableMap.of()));
    workerMap =
        ImmutableMap.of(
            2L,
            StoredWorker.newBuilder().setNode(Node.newBuilder().setId(2L).build()).build(),
            3L,
            StoredWorker.newBuilder().setNode(Node.newBuilder().setId(3L).build()).build());
    rebalancer = new Rebalancer() {};
  }

  @Test
  public void computeWorkerId() throws Exception {
    rebalancer.computeWorkerId(jobGroupMap, workerMap);
    Versioned<StoredJobGroup> got = jobGroupMap.get("test_topic").toStoredJobGroup();
    Assert.assertEquals(1, got.version());
    Assert.assertNotEquals(0L, got.model().getJobs(0).getWorkerId());
    Assert.assertNotEquals(1L, got.model().getJobs(0).getWorkerId());
    Assert.assertEquals(2L, got.model().getJobs(1).getWorkerId());
  }

  @Test
  public void computeWorkerIdWithoutWorkers() throws Exception {
    rebalancer.computeJobState(jobGroupMap, ImmutableMap.of());
    // verify no NPE.
  }

  @Test
  public void computeJobConfiguration() throws Exception {
    rebalancer.computeJobConfiguration(jobGroupMap, workerMap);

    Versioned<StoredJobGroup> got = jobGroupMap.get("test_topic").toStoredJobGroup();
    Assert.assertEquals(1, got.version());

    FlowControl expected =
        FlowControl.newBuilder()
            .setMaxInflightMessages(1)
            .setMessagesPerSec(2)
            .setBytesPerSec(4)
            .build();
    Assert.assertEquals(expected, got.model().getJobs(0).getJob().getFlowControl());
    Assert.assertEquals(expected, got.model().getJobs(1).getJob().getFlowControl());
    Assert.assertEquals(1.0, got.model().getJobs(0).getScale(), 0.0001);
    Assert.assertEquals(1.0, got.model().getJobs(1).getScale(), 0.0001);
  }

  @Test
  public void computeJobState() throws Exception {
    rebalancer.computeJobState(jobGroupMap, workerMap);

    Versioned<StoredJobGroup> got = jobGroupMap.get("test_topic").toStoredJobGroup();
    Assert.assertEquals(1, got.version());

    Assert.assertEquals(JobState.JOB_STATE_RUNNING, got.model().getJobs(0).getState());
    Assert.assertEquals(JobState.JOB_STATE_RUNNING, got.model().getJobs(1).getState());
  }

  @Test
  public void testPostProcess() throws Exception {
    rebalancer.postProcess(jobGroupMap, workerMap);
  }

  @Test
  public void testMergeJobGroupAndJob() {
    String jobGroupString = "jobGroup";
    String resqTopic = "resq-topic";
    String resqCluster = "resq-cluster";
    int jobGroupInt = 1;
    boolean jobGroupBoolean = false;
    List<String> jobGroupList = Arrays.asList("jobGroup1");
    List<RetryQueue> jobGroupRetryQueues =
        Arrays.asList(
            RetryQueue.newBuilder()
                .setRetryQueueTopic(jobGroupString)
                .setRetryCluster(jobGroupString)
                .setProcessingDelayMs(jobGroupInt)
                .setMaxRetryCount(jobGroupInt)
                .build());
    JobGroup.Builder jobGroupBuilder = JobGroup.newBuilder();
    jobGroupBuilder.setJobGroupId(jobGroupString);
    jobGroupBuilder.setType(JobType.JOB_TYPE_INVALID);
    jobGroupBuilder
        .getKafkaConsumerTaskGroupBuilder()
        .setCluster(jobGroupString)
        .setTopic(jobGroupString)
        .setCluster(jobGroupString)
        .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID)
        .setStartTimestamp(Timestamps.fromSeconds(jobGroupInt))
        .setEndTimestamp(Timestamps.fromSeconds(jobGroupInt))
        .setProcessingDelayMs(jobGroupInt);
    jobGroupBuilder
        .getRpcDispatcherTaskGroupBuilder()
        .setUri(jobGroupString)
        .setProcedure(jobGroupString)
        .setRpcTimeoutMs(jobGroupInt)
        .setMaxRpcTimeouts(jobGroupInt)
        .setRetryQueueTopic(jobGroupString)
        .setRetryCluster(jobGroupString)
        .setDlqTopic(jobGroupString)
        .setDlqCluster(jobGroupString);
    jobGroupBuilder
        .getFlowControlBuilder()
        .setMessagesPerSec(jobGroupInt)
        .setBytesPerSec(jobGroupInt)
        .setMaxInflightMessages(jobGroupInt);
    jobGroupBuilder
        .getSecurityConfigBuilder()
        .setIsSecure(jobGroupBoolean)
        .addAllServiceIdentities(jobGroupList);
    jobGroupBuilder
        .getRetryConfigBuilder()
        .setRetryEnabled(jobGroupBoolean)
        .addAllRetryQueues(jobGroupRetryQueues);
    jobGroupBuilder
        .getResqConfigBuilder()
        .setResqEnabled(true)
        .setResqTopic(resqTopic)
        .setResqCluster(resqCluster)
        .getFlowControlBuilder()
        .setMessagesPerSec(jobGroupInt)
        .setBytesPerSec(jobGroupInt)
        .setMaxInflightMessages(jobGroupInt);
    jobGroupBuilder.getMiscConfigBuilder().setOwnerServiceName("test-service").setEnableDebug(true);
    jobGroupBuilder.setExtension(
        Any.pack(MiscConfig.newBuilder().setOwnerServiceName("abc").build()));
    JobGroup jobGroup = jobGroupBuilder.build();

    String jobString = "job";
    int jobInt = 2;
    boolean jobBoolean = true;
    List<String> jobList = Arrays.asList("job1");
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.setJobId(jobInt);
    jobBuilder
        .getKafkaConsumerTaskBuilder()
        .setTopic(jobString)
        .setCluster(jobString)
        .setConsumerGroup(jobString)
        .setPartition(jobInt)
        .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST)
        .setStartOffset(jobInt)
        .setEndOffset(jobInt)
        .setProcessingDelayMs(jobInt);
    jobBuilder
        .getRpcDispatcherTaskBuilder()
        .setUri(jobString)
        .setProcedure(jobString)
        .setRpcTimeoutMs(jobInt)
        .setMaxRpcTimeouts(jobInt)
        .setRetryQueueTopic(jobString)
        .setRetryCluster(jobString)
        .setDlqTopic(jobString)
        .setDlqCluster(jobString);
    jobBuilder
        .getFlowControlBuilder()
        .setMessagesPerSec(jobInt)
        .setBytesPerSec(jobInt)
        .setMaxInflightMessages(jobInt);
    jobBuilder.getSecurityConfigBuilder().setIsSecure(jobBoolean).addAllServiceIdentities(jobList);
    jobBuilder
        .getRetryConfigBuilder()
        .setRetryEnabled(jobBoolean)
        .addAllRetryQueues(jobGroupRetryQueues);
    jobBuilder.getMiscConfigBuilder().setEnableDebug(false);
    Job job = jobBuilder.build();

    Job newJob = Rebalancer.mergeJobGroupAndJob(jobGroup, job).build();

    Job.Builder expectedBuilder = Job.newBuilder();
    expectedBuilder.setJobId(jobInt);
    expectedBuilder
        .getFlowControlBuilder()
        .setBytesPerSec(jobInt)
        .setMessagesPerSec(jobInt)
        .setMaxInflightMessages(jobInt);
    expectedBuilder
        .getKafkaConsumerTaskBuilder()
        .setConsumerGroup(jobString)
        .setCluster(jobString)
        .setTopic(jobString)
        .setPartition(jobInt)
        .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST)
        .setStartOffset(jobInt)
        .setEndOffset(jobInt)
        .setProcessingDelayMs(jobGroupInt);
    expectedBuilder
        .getRpcDispatcherTaskBuilder()
        .setUri(jobString)
        .setProcedure(jobGroupString)
        .setRpcTimeoutMs(jobGroupInt)
        .setMaxRpcTimeouts(jobGroupInt)
        .setRetryQueueTopic(jobGroupString)
        .setRetryCluster(jobString)
        .setDlqTopic(jobGroupString)
        .setDlqCluster(jobString);
    expectedBuilder
        .getSecurityConfigBuilder()
        .setIsSecure(jobBoolean)
        .addAllServiceIdentities(jobGroupList);
    expectedBuilder
        .getRetryConfigBuilder()
        .setRetryEnabled(jobGroupBoolean)
        .addAllRetryQueues(jobGroupRetryQueues);
    expectedBuilder
        .getResqConfigBuilder()
        .setResqEnabled(true)
        .setResqTopic(resqTopic)
        .setResqCluster(resqCluster)
        .getFlowControlBuilder()
        .setMessagesPerSec(jobGroupInt)
        .setBytesPerSec(jobGroupInt)
        .setMaxInflightMessages(jobGroupInt);
    expectedBuilder.getMiscConfigBuilder().setOwnerServiceName("test-service").setEnableDebug(true);
    expectedBuilder.setExtension(
        Any.pack(MiscConfig.newBuilder().setOwnerServiceName("abc").build()));
    Assert.assertEquals(expectedBuilder.build(), newJob);
  }

  @Test
  public void testMergeJobGroupAndJobForLoadGenProduceJobType() {
    String cluster = "test-cluster";
    String topic = "test-topic";
    int partition = 0;
    long jobId = 1L;

    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setJobGroupId("test-jg1")
            .setType(JobType.JOB_TYPE_LOAD_GEN_PRODUCE)
            .setKafkaDispatcherTaskGroup(
                KafkaDispatcherTaskGroup.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setDedupEnabled(true)
                    .build())
            .build();

    Job job =
        Job.newBuilder()
            .setJobId(jobId)
            .setType(JobType.JOB_TYPE_LOAD_GEN_PRODUCE)
            .setKafkaDispatcherTask(
                KafkaDispatcherTask.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setDedupEnabled(false)
                    .setPartition(partition)
                    .build())
            .build();

    Job expectedJob =
        Job.newBuilder()
            .setJobId(jobId)
            .setType(JobType.JOB_TYPE_LOAD_GEN_PRODUCE)
            .setKafkaDispatcherTask(
                KafkaDispatcherTask.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setDedupEnabled(false)
                    .setPartition(partition)
                    .setEncodedFormatInfo(EncodedFormatInfo.newBuilder().build())
                    .build())
            .setFlowControl(FlowControl.newBuilder().build())
            .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().build())
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().build())
            .setSecurityConfig(SecurityConfig.newBuilder().build())
            .setRetryConfig(RetryConfig.newBuilder().build())
            .setResqConfig(ResqConfig.newBuilder().build())
            .setMiscConfig(MiscConfig.newBuilder().build())
            .setExtension(Any.getDefaultInstance())
            .build();

    Job actualJob = Rebalancer.mergeJobGroupAndJob(jobGroup, job).build();
    Assert.assertEquals(expectedJob, actualJob);
  }

  @Test
  public void testMergeJobGroupAndJobForKafkaAuditJobType() {
    String cluster = "test-cluster";
    String topic = "test-topic";
    int partition = 0;
    int auditIntervalInSeconds = 60;
    long jobId = 1L;

    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setJobGroupId("test-jg1")
            .setType(JobType.JOB_TYPE_KAFKA_AUDIT)
            .setAuditTaskGroup(
                AuditTaskGroup.newBuilder().setCluster(cluster).setTopic(topic).build())
            .build();

    Job job =
        Job.newBuilder()
            .setJobId(jobId)
            .setType(JobType.JOB_TYPE_KAFKA_AUDIT)
            .setAuditTask(
                AuditTask.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setPartition(partition)
                    .setAuditMetadata(
                        AuditMetaData.newBuilder()
                            .addAuditConfigs(
                                AuditConfig.newBuilder()
                                    .setAuditIntervalInSeconds(auditIntervalInSeconds)
                                    .setAuditType(AuditType.AUDIT_TYPE_XDC)
                                    .build())
                            .build())
                    .build())
            .build();

    Job expectedJob =
        Job.newBuilder()
            .setJobId(jobId)
            .setType(JobType.JOB_TYPE_KAFKA_AUDIT)
            .setAuditTask(
                AuditTask.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setPartition(partition)
                    .setAuditMetadata(
                        AuditMetaData.newBuilder()
                            .addAuditConfigs(
                                AuditConfig.newBuilder()
                                    .setAuditIntervalInSeconds(auditIntervalInSeconds)
                                    .setAuditType(AuditType.AUDIT_TYPE_XDC)
                                    .build())
                            .build())
                    .build())
            .setFlowControl(FlowControl.newBuilder().build())
            .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().build())
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().build())
            .setSecurityConfig(SecurityConfig.newBuilder().build())
            .setRetryConfig(RetryConfig.newBuilder().build())
            .setResqConfig(ResqConfig.newBuilder().build())
            .setMiscConfig(MiscConfig.newBuilder().build())
            .setExtension(Any.getDefaultInstance())
            .build();

    Job actualJob = Rebalancer.mergeJobGroupAndJob(jobGroup, job).build();
    Assert.assertEquals(expectedJob, actualJob);
  }

  @Test
  public void testMergeJobGroupAndJobForLoadGenConsumeJobType() {
    String cluster = "test-cluster";
    String topic = "test-topic";
    int partition = 0;
    String consumerGroup = "test-cg";
    long jobId = 1L;

    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setJobGroupId("test-jg1")
            .setType(JobType.JOB_TYPE_LOAD_GEN_CONSUME)
            .setKafkaConsumerTaskGroup(
                KafkaConsumerTaskGroup.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setConsumerGroup(consumerGroup)
                    .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST)
                    .build())
            .build();

    Job job =
        Job.newBuilder()
            .setJobId(jobId)
            .setType(JobType.JOB_TYPE_LOAD_GEN_CONSUME)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setPartition(partition)
                    .setConsumerGroup(consumerGroup)
                    .setAutoOffsetResetPolicy(
                        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST))
            .build();

    Job expectedJob =
        Job.newBuilder()
            .setJobId(jobId)
            .setType(JobType.JOB_TYPE_LOAD_GEN_CONSUME)
            .setFlowControl(FlowControl.newBuilder().build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setPartition(partition)
                    .setConsumerGroup(consumerGroup)
                    .setAutoOffsetResetPolicy(
                        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST))
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().build())
            .setSecurityConfig(SecurityConfig.newBuilder().build())
            .setRetryConfig(RetryConfig.newBuilder().build())
            .setResqConfig(ResqConfig.newBuilder().build())
            .setMiscConfig(MiscConfig.newBuilder().build())
            .setExtension(Any.getDefaultInstance())
            .build();

    Job actualJob = Rebalancer.mergeJobGroupAndJob(jobGroup, job).build();
    Assert.assertEquals(expectedJob, actualJob);
  }

  @Test
  public void testMergeJobGroupAndJobForKafkaReplicationJobType() {
    String cluster = "test-cluster";
    String topic = "test-topic";
    String dstCluster = "dst-test-cluster";
    String dstTopic = "dst-test-topic";
    int partition = 0;
    String consumerGroup = "test-cg";
    long jobId = 1L;

    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setJobGroupId("test-jg1")
            .setType(JobType.JOB_TYPE_KAFKA_REPLICATION)
            .setKafkaConsumerTaskGroup(
                KafkaConsumerTaskGroup.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setConsumerGroup(consumerGroup)
                    .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST)
                    .build())
            .build();

    Job job =
        Job.newBuilder()
            .setJobId(jobId)
            .setType(JobType.JOB_TYPE_KAFKA_REPLICATION)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setPartition(partition)
                    .setConsumerGroup(consumerGroup)
                    .setAutoOffsetResetPolicy(
                        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST))
            .setKafkaDispatcherTask(
                KafkaDispatcherTask.newBuilder()
                    .setCluster(dstCluster)
                    .setTopic(dstTopic)
                    .setPartition(partition)
                    .setDedupEnabled(false))
            .build();

    Job expectedJob =
        Job.newBuilder()
            .setJobId(jobId)
            .setType(JobType.JOB_TYPE_KAFKA_REPLICATION)
            .setFlowControl(FlowControl.newBuilder().build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster(cluster)
                    .setTopic(topic)
                    .setPartition(partition)
                    .setConsumerGroup(consumerGroup)
                    .setAutoOffsetResetPolicy(
                        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST))
            .setKafkaDispatcherTask(
                KafkaDispatcherTask.newBuilder()
                    .setCluster(dstCluster)
                    .setTopic(dstTopic)
                    .setPartition(partition)
                    .setDedupEnabled(false)
                    .setEncodedFormatInfo(EncodedFormatInfo.newBuilder().build()))
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().build())
            .setSecurityConfig(SecurityConfig.newBuilder().build())
            .setRetryConfig(RetryConfig.newBuilder().build())
            .setResqConfig(ResqConfig.newBuilder().build())
            .setMiscConfig(MiscConfig.newBuilder().build())
            .setExtension(Any.getDefaultInstance())
            .build();

    Job actualJob = Rebalancer.mergeJobGroupAndJob(jobGroup, job).build();
    Assert.assertEquals(expectedJob, actualJob);
  }

  @Test
  public void testMergeJobGroupAndJobForKafkaAvailabilityJobType() {
    String TEST_CLUSTER = "cluster";
    String TEST_TOPIC = "topic";
    JobType TEST_JOB_TYPE = JobType.JOB_TYPE_KAFKA_AVAILABILITY;
    AvailabilityJobType TEST_KA_JOB_TYPE =
        AvailabilityJobType.AVAILABILITY_JOB_TYPE_NATIVE_PRODUCER;
    int TEST_PARTITION = 0;
    long TEST_JOB_ID = 1L;

    JobGroup jobGroup =
        JobGroup.newBuilder()
            .setType(TEST_JOB_TYPE)
            .setKafkaDispatcherTaskGroup(
                KafkaDispatcherTaskGroup.newBuilder()
                    .setCluster(TEST_CLUSTER)
                    .setTopic(TEST_TOPIC)
                    .build())
            .setAvailabilityTaskGroup(
                AvailabilityTaskGroup.newBuilder()
                    .setAvailabilityJobType(TEST_KA_JOB_TYPE)
                    .setZoneIsolated(true)
                    .build())
            .build();

    Job job =
        Job.newBuilder()
            .setJobId(TEST_JOB_ID)
            .setType(TEST_JOB_TYPE)
            .setKafkaDispatcherTask(
                KafkaDispatcherTask.newBuilder()
                    .setCluster(TEST_CLUSTER)
                    .setTopic(TEST_TOPIC)
                    .setPartition(TEST_PARTITION)
                    .setDedupEnabled(false)
                    .setIsSecure(false)
                    .setIsAcksOne(true)
                    .setEncodedFormatInfo(
                        EncodedFormatInfo.newBuilder()
                            .setEncodedFormatType(EncodedFormatType.ENCODED_FORMAT_TYPE_PROTOBUF)
                            .setSchemaVersion(1)
                            .build())
                    .build())
            .setAvailabilityTask(
                AvailabilityTask.newBuilder()
                    .setAvailabilityJobType(TEST_KA_JOB_TYPE)
                    .setZoneIsolated(true)
                    .build())
            .build();

    Job expectedJob =
        Job.newBuilder()
            .setJobId(TEST_JOB_ID)
            .setType(TEST_JOB_TYPE)
            .setFlowControl(FlowControl.newBuilder().build())
            .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().build())
            .setKafkaDispatcherTask(
                KafkaDispatcherTask.newBuilder()
                    .setCluster(TEST_CLUSTER)
                    .setTopic(TEST_TOPIC)
                    .setPartition(TEST_PARTITION)
                    .setDedupEnabled(false)
                    .setIsSecure(false)
                    .setIsAcksOne(true)
                    .setEncodedFormatInfo(
                        EncodedFormatInfo.newBuilder()
                            .setSchemaVersion(1)
                            .setEncodedFormatType(EncodedFormatType.ENCODED_FORMAT_TYPE_PROTOBUF)
                            .build())
                    .build())
            .setAvailabilityTask(
                AvailabilityTask.newBuilder()
                    .setAvailabilityJobType(TEST_KA_JOB_TYPE)
                    .setZoneIsolated(true)
                    .build())
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().build())
            .setSecurityConfig(SecurityConfig.newBuilder().build())
            .setRetryConfig(RetryConfig.newBuilder().build())
            .setResqConfig(ResqConfig.newBuilder().build())
            .setMiscConfig(MiscConfig.newBuilder().build())
            .setExtension(Any.getDefaultInstance())
            .build();

    Job actualJob = Rebalancer.mergeJobGroupAndJob(jobGroup, job).build();
    Assert.assertEquals(expectedJob, actualJob);
  }
}
