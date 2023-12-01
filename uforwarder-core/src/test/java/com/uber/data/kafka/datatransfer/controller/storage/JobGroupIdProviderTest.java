package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobGroupIdProviderTest extends FievelTestBase {
  private Job.Builder kafkaToRpcJob;
  private JobGroupIdProvider jobGroupKeyProvider;

  @Before
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
    Assert.assertTrue(jobGroupKeyProvider.isRunning());
    jobGroupKeyProvider.stop();
  }

  @Test
  public void getKeyForJobGroup() {
    Assert.assertEquals(
        "jobGroupId",
        jobGroupKeyProvider.getId(JobGroup.newBuilder().setJobGroupId("jobGroupId").build()));
  }

  @Test
  public void testGetIdForStoredJobGroup() throws Exception {
    Assert.assertEquals(
        "jobGroupId",
        jobGroupKeyProvider.getId(
            StoredJobGroup.newBuilder()
                .setJobGroup(JobGroup.newBuilder().setJobGroupId("jobGroupId").build())
                .build()));
  }
}
