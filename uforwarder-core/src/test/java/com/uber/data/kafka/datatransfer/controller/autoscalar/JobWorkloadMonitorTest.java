package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.data.kafka.datatransfer.controller.rpc.Workload;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import java.time.Duration;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobWorkloadMonitorTest extends FievelTestBase {
  private JobWorkloadMonitor jobWorkloadMonitor;
  private TestUtils.TestTicker testTicker;
  private Job job1;
  private Job job2;
  private JobGroup jobGroup;
  private final String topic = "topic1";
  private final String cluster = "cluster1";
  private final String group = "group1";

  @Before
  public void setup() {
    testTicker = new TestUtils.TestTicker();
    AutoScalarConfiguration configuration = new AutoScalarConfiguration();
    configuration.setThroughputTTL(Duration.ofNanos(1000));
    configuration.setJobStatusTTL(Duration.ofMinutes(5));
    jobWorkloadMonitor = new JobWorkloadMonitor(configuration, testTicker, new NoopScope());
    job1 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(group)
                    .setTopic(topic)
                    .setCluster(cluster)
                    .setPartition(1)
                    .build())
            .build();

    job2 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(group)
                    .setTopic(topic)
                    .setCluster(cluster)
                    .setPartition(2)
                    .build())
            .build();

    jobGroup =
        JobGroup.newBuilder()
            .setKafkaConsumerTaskGroup(
                KafkaConsumerTaskGroup.newBuilder()
                    .setConsumerGroup(group)
                    .setTopic(topic)
                    .setCluster(cluster)
                    .build())
            .build();
  }

  @Test
  public void testUpdateAndGet() {
    jobWorkloadMonitor.get(JobGroupKey.of(jobGroup));
    jobWorkloadMonitor.consume(job1, Workload.of(1, 5, 0));
    Optional<Workload> result = jobWorkloadMonitor.get(JobGroupKey.of(jobGroup));
    Assert.assertEquals(1, result.get().getMessagesPerSecond(), 0.0001);
    Assert.assertEquals(5, result.get().getBytesPerSecond(), 0.0001);
  }

  @Test
  public void testUpdateAndGetMultiJob() {
    jobWorkloadMonitor.get(JobGroupKey.of(jobGroup));
    jobWorkloadMonitor.consume(job1, Workload.of(1, 5, 0));
    jobWorkloadMonitor.consume(job2, Workload.of(3, 25, 0));
    Optional<Workload> result = jobWorkloadMonitor.get(JobGroupKey.of(jobGroup));
    Assert.assertEquals(4, result.get().getMessagesPerSecond(), 0.0001);
    Assert.assertEquals(30, result.get().getBytesPerSecond(), 0.0001);
  }

  @Test
  public void testUpdateAndGetExpired() {
    jobWorkloadMonitor.get(JobGroupKey.of(jobGroup));
    jobWorkloadMonitor.consume(job1, Workload.of(1, 5, 0));
    testTicker.add(Duration.ofSeconds(2));
    jobWorkloadMonitor.cleanUp();
    Optional<Workload> result = jobWorkloadMonitor.get(JobGroupKey.of(jobGroup));
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void testCleanup() {
    jobWorkloadMonitor.get(JobGroupKey.of(jobGroup));
    jobWorkloadMonitor.consume(job1, Workload.of(1, 5, 0));
    Optional<Workload> result = jobWorkloadMonitor.get(JobGroupKey.of(jobGroup));
    Assert.assertEquals(1, result.get().getMessagesPerSecond(), 0.0001);
    Assert.assertEquals(5, result.get().getBytesPerSecond(), 0.0001);
    testTicker.add(Duration.ofMinutes(6));
    jobWorkloadMonitor.cleanUp();
    Assert.assertTrue(jobWorkloadMonitor.getJobGroupWorkloadMap().isEmpty());
  }
}
