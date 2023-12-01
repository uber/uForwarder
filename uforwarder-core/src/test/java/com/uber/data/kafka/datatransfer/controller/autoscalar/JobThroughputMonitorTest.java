package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import java.time.Duration;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JobThroughputMonitorTest extends FievelTestBase {
  private JobThroughputMonitor jobThroughputMonitor;
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
    jobThroughputMonitor = new JobThroughputMonitor(configuration, testTicker, new NoopScope());
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
    jobThroughputMonitor.get(JobGroupKey.of(jobGroup));
    jobThroughputMonitor.consume(job1, 1, 5);
    Optional<Throughput> result = jobThroughputMonitor.get(JobGroupKey.of(jobGroup));
    Assert.assertEquals(1, result.get().getMessagesPerSecond(), 0.0001);
    Assert.assertEquals(5, result.get().getBytesPerSecond(), 0.0001);
  }

  @Test
  public void testUpdateAndGetMultiJob() {
    jobThroughputMonitor.get(JobGroupKey.of(jobGroup));
    jobThroughputMonitor.consume(job1, 1, 5);
    jobThroughputMonitor.consume(job2, 3, 25);
    Optional<Throughput> result = jobThroughputMonitor.get(JobGroupKey.of(jobGroup));
    Assert.assertEquals(4, result.get().getMessagesPerSecond(), 0.0001);
    Assert.assertEquals(30, result.get().getBytesPerSecond(), 0.0001);
  }

  @Test
  public void testUpdateAndGetExpired() {
    jobThroughputMonitor.get(JobGroupKey.of(jobGroup));
    jobThroughputMonitor.consume(job1, 1, 5);
    testTicker.add(Duration.ofSeconds(2));
    jobThroughputMonitor.cleanUp();
    Optional<Throughput> result = jobThroughputMonitor.get(JobGroupKey.of(jobGroup));
    Assert.assertFalse(result.isPresent());
  }

  @Test
  public void testCleanup() {
    jobThroughputMonitor.get(JobGroupKey.of(jobGroup));
    jobThroughputMonitor.consume(job1, 1, 5);
    Optional<Throughput> result = jobThroughputMonitor.get(JobGroupKey.of(jobGroup));
    Assert.assertEquals(1, result.get().getMessagesPerSecond(), 0.0001);
    Assert.assertEquals(5, result.get().getBytesPerSecond(), 0.0001);
    testTicker.add(Duration.ofMinutes(6));
    jobThroughputMonitor.cleanUp();
    Assert.assertTrue(jobThroughputMonitor.getJobGroupThroughputMap().isEmpty());
  }
}
