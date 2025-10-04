package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JobGroupIdProviderTest {
  private Job.Builder kafkaToRpcJob;
  private JobGroupIdProvider jobGroupKeyProvider;

  @BeforeEach
  public void setup() {
    jobGroupKeyProvider = new JobGroupIdProvider();
    kafkaToRpcJob = Job.newBuilder();
    kafkaToRpcJob.setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER);

    // The following fields are included in the path name
    kafkaToRpcJob.getKafkaConsumerTaskBuilder().setConsumerGroup("group");
    kafkaToRpcJob.getKafkaConsumerTaskBuilder().setTopic("topic");
    kafkaToRpcJob.getKafkaConsumerTaskBuilder().setCluster("cluster");
    kafkaToRpcJob.getRpcDispatcherTaskBuilder().setUri("grpc://service-name");
    // The following are excluded and we should validate that fact
    kafkaToRpcJob.setFlowControl(
        FlowControl.newBuilder().setMessagesPerSec(100).setBytesPerSec(200).build());
  }

  @Test
  public void lifecycle() {
    jobGroupKeyProvider.start();
    Assertions.assertTrue(jobGroupKeyProvider.isRunning());
    jobGroupKeyProvider.stop();
  }

  @Test
  public void getKeyForJobGroup() {
    Assertions.assertEquals(
        "jobGroupId",
        jobGroupKeyProvider.getId(JobGroup.newBuilder().setJobGroupId("jobGroupId").build()));
  }

  @Test
  public void testGetIdForStoredJobGroup() throws Exception {
    Assertions.assertEquals(
        "jobGroupId",
        jobGroupKeyProvider.getId(
            StoredJobGroup.newBuilder()
                .setJobGroup(JobGroup.newBuilder().setJobGroupId("jobGroupId").build())
                .build()));
  }
}
