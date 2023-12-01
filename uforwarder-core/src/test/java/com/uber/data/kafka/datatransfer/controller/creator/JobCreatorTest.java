package com.uber.data.kafka.datatransfer.controller.creator;

import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.JobUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class JobCreatorTest extends FievelTestBase {
  @Test
  public void test() {
    JobCreator jobCreator = new JobCreator() {};

    StoredJobGroup storedJobGroup =
        StoredJobGroup.newBuilder()
            .setJobGroup(
                JobGroup.newBuilder()
                    .setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER)
                    .setKafkaConsumerTaskGroup(
                        KafkaConsumerTaskGroup.newBuilder()
                            .setIsolationLevel(IsolationLevel.ISOLATION_LEVEL_READ_COMMITTED)
                            .build())
                    .build())
            .setState(JobState.JOB_STATE_RUNNING)
            .build();

    // TODO: Assert Job.IsLosslessTopic once it is propagate to StoredJob.Job
    StoredJob storedJob = jobCreator.newJob(storedJobGroup, 1, 2);
    Assert.assertEquals(1, storedJob.getJob().getJobId());
    Assert.assertEquals(2, JobUtils.getJobKey(storedJob));
    Assert.assertEquals(-1, storedJob.getJob().getKafkaConsumerTask().getStartOffset());
    Assert.assertEquals(0, storedJob.getJob().getKafkaConsumerTask().getEndOffset());
    Assert.assertEquals(
        IsolationLevel.ISOLATION_LEVEL_READ_COMMITTED,
        storedJob.getJob().getKafkaConsumerTask().getIsolationLevel());
    Assert.assertEquals(JobState.JOB_STATE_RUNNING, storedJob.getState());
  }
}
