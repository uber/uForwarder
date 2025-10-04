package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.MessageOrBuilder;
import com.uber.data.kafka.datatransfer.AutoScalarSnapshot;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.MiscConfig;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.data.kafka.datatransfer.controller.rpc.Workload;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import java.time.Duration;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AutoScalarTest extends FievelTestBase {
  private AutoScalar autoScalar;
  private AutoScalarConfiguration config;
  private JobWorkloadMonitor jobWorkloadMonitor;
  private LeaderSelector leaderSelector;
  private TestUtils.TestTicker testTicker;
  private Job job;
  private JobGroup jobGroup;
  private String TOPIC = "topic";
  private String GROUP = "group";
  private String CLUSTER = "cluster";
  private RebalancingJobGroup rebalancingJobGroup;

  @Before
  public void setup() {
    testTicker = new TestUtils.TestTicker();
    config = new AutoScalarConfiguration();
    config.setDryRun(false);
    config.setHibernatingEnabled(true);
    config.setMessagesPerSecPerWorker(2000);
    config.setBytesPerSecPerWorker(50000);
    config.setJobStatusTTL(Duration.ofDays(2));
    config.setUpScaleMaxFactor(1.4);
    config.setUpScaleMinFactor(1.1);
    config.setDownScaleMaxFactor(0.9);
    config.setDownScaleMinFactor(0.8);
    jobWorkloadMonitor = new JobWorkloadMonitor(config, testTicker, new NoopScope());
    leaderSelector = Mockito.mock(LeaderSelector.class);
    Mockito.when(leaderSelector.isLeader()).thenReturn(true);
    autoScalar =
        new AutoScalar(config, jobWorkloadMonitor, testTicker, new NoopScope(), leaderSelector);
    job =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setTopic(TOPIC)
                    .setConsumerGroup(GROUP)
                    .setCluster(CLUSTER)
                    .setPartition(0)
                    .build())
            .build();
    jobGroup =
        JobGroup.newBuilder()
            .setKafkaConsumerTaskGroup(
                KafkaConsumerTaskGroup.newBuilder()
                    .setTopic(TOPIC)
                    .setConsumerGroup(GROUP)
                    .setCluster(CLUSTER)
                    .build())
            .setFlowControl(
                FlowControl.newBuilder().setMessagesPerSec(4000).setBytesPerSec(10000).build())
            .setMiscConfig(MiscConfig.newBuilder().setScaleResetEnabled(true).build())
            .build();
    rebalancingJobGroup =
        RebalancingJobGroup.of(
            Versioned.from(
                StoredJobGroup.newBuilder()
                    .setJobGroup(jobGroup)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build(),
                1),
            ImmutableMap.of());
  }

  @Test
  public void testScaleUpAboveMaxFactor() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(i < 28 ? 4000 : 8000, 10000, 0.0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2.792, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        5584, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        139600, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testScaleUpAboveMaxFactorWithCpuUsage() {
    config.setCpuUsagePerWorker(2.0);
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(1, 1, i < 28 ? 4.0 : 8.0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2.792, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        5584, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        139600, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testScaleUpBelowMinFactor() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(4100, 10000, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testScaleUpBelowMinFactorWithCpuUsage() {
    config.setCpuUsagePerWorker(2.0);
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(1, 1, 4.1));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testScaleUp20Percent() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(i < 28 ? 4000 : 4800, 10000, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2.4, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4800, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        120000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testScaleUp20PercentWIthCpuUsage() {
    config.setCpuUsagePerWorker(2.0);
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(1, 1, i < 28 ? 4.0 : 4.8));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2.4, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4800, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        120000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testScaleDownBelowMin() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      jobWorkloadMonitor.consume(job, Workload.of(i < samples - 10 ? 1000 : 4000, 800, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(1.6, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        3200, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        80000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testSmallScaleNotScaleDown() {
    rebalancingJobGroup.updateScale(1e-8, new Throughput(2e-5, 4.5e-4));
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(1e-8, rebalancingJobGroup.getScale().get(), 1e-9);
    Assert.assertEquals(
        2e-5, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 1e-9);
    Assert.assertEquals(
        4.5e-4, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 1e-9);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      jobWorkloadMonitor.consume(job, Workload.of(1e-9, 1e-9, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(1e-8, rebalancingJobGroup.getScale().get(), 1e-9);
    Assert.assertEquals(
        2e-5, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 1e-9);
    Assert.assertEquals(
        4.5e-4, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 1e-9);
  }

  @Test
  public void testScaleDownAboveMax() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      jobWorkloadMonitor.consume(job, Workload.of(3800, 800, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testScaleDown15Percent() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      jobWorkloadMonitor.consume(job, Workload.of(3400, 800, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(1.7, rebalancingJobGroup.getScale().get(), 0.01);
    Assert.assertEquals(
        3400, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        85000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testHibernatingAndBootstrap() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    int samples = 3 * 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofMinutes(1));
      jobWorkloadMonitor.consume(job, Workload.of(0, 0, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertTrue(0.0 == rebalancingJobGroup.getScale().get());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getMessagesPerSecond());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getBytesPerSecond());
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(1000, 10000, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(0.5, rebalancingJobGroup.getScale().get(), 0.0001);
    Assert.assertEquals(
        1000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        25000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testHibernatingAndBootstrapWithCpuUsage() {
    config.setCpuUsagePerWorker(2.0);
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    int samples = 3 * 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofMinutes(1));
      jobWorkloadMonitor.consume(job, Workload.of(0, 0, 1));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertTrue(0.0 == rebalancingJobGroup.getScale().get());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getMessagesPerSecond());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getBytesPerSecond());
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(1000, 10000, 1));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(0.5, rebalancingJobGroup.getScale().get(), 0.0001);
    Assert.assertEquals(
        1000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        25000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testHibernating() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    int samples = 3 * 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofMinutes(1));
      jobWorkloadMonitor.consume(job, Workload.of(0, 0, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertTrue(0.0 == rebalancingJobGroup.getScale().get());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getMessagesPerSecond());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getBytesPerSecond());
  }

  @Test
  public void testHibernatingSmallScale() {
    rebalancingJobGroup.updateScale(10e-8, new Throughput(2e-5, 4.5e-4));
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(10e-8, rebalancingJobGroup.getScale().get(), 10e-10);
    Assert.assertEquals(
        2e-5, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 1e-9);
    Assert.assertEquals(
        4.5e-4, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 1e-9);
    int samples = 3 * 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofMinutes(1));
      jobWorkloadMonitor.consume(job, Workload.of(0, 0, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertTrue(0.0 == rebalancingJobGroup.getScale().get());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getMessagesPerSecond());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getBytesPerSecond());
  }

  @Test
  public void testBootstrap() {
    rebalancingJobGroup.updateScale(0.0d, Throughput.ZERO);
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertTrue(0.0 == rebalancingJobGroup.getScale().get());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getMessagesPerSecond());
    Assert.assertTrue(0.0 == rebalancingJobGroup.getThroughput().get().getBytesPerSecond());
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(1000, 10000, 0));
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(0.5, rebalancingJobGroup.getScale().get(), 0.0001);
    Assert.assertEquals(
        1000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        25000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testStopReportThroughput() {
    // stop report throughout stops scaling
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    jobWorkloadMonitor.consume(job, Workload.of(3400, 800, 0));
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      // no throughput reported
      autoScalar.runSample();
    }
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2.0, rebalancingJobGroup.getScale().get(), 0.01);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testUpdateQuotaResetScale() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        4000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        100000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);

    testTicker.add(Duration.ofSeconds(60));
    jobWorkloadMonitor.consume(job, Workload.of(3400, 800, 0));
    autoScalar.runSample();

    // scale reset is disabled
    jobGroup =
        JobGroup.newBuilder(jobGroup)
            .setFlowControl(
                FlowControl.newBuilder().setMessagesPerSec(6000).setBytesPerSec(10000).build())
            .setMiscConfig(MiscConfig.newBuilder().setScaleResetEnabled(false).build())
            .build();

    rebalancingJobGroup =
        RebalancingJobGroup.of(
            Versioned.from(
                StoredJobGroup.newBuilder()
                    .setJobGroup(jobGroup)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build(),
                1),
            ImmutableMap.of());
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);

    // scale reset is enabled
    jobGroup =
        JobGroup.newBuilder(jobGroup)
            .setMiscConfig(MiscConfig.newBuilder().setScaleResetEnabled(true).build())
            .build();
    rebalancingJobGroup =
        RebalancingJobGroup.of(
            Versioned.from(
                StoredJobGroup.newBuilder()
                    .setJobGroup(jobGroup)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build(),
                1),
            ImmutableMap.of());
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(3, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        6000, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        150000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testCleanupNonRunningJobs() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    autoScalar.runSample();
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertFalse(jobWorkloadMonitor.getJobGroupWorkloadMap().isEmpty());
    Assert.assertFalse(autoScalar.getStatusStore().isEmpty());
    // not expired
    testTicker.add(Duration.ofHours(47));
    autoScalar.runSample();
    autoScalar.cleanUp();
    Assert.assertFalse(autoScalar.getStatusStore().isEmpty());

    // expired
    testTicker.add(Duration.ofHours(2));
    autoScalar.runSample(); // refresh last access time of  jobThroughputMonitor
    autoScalar.cleanUp();
    Assert.assertTrue(autoScalar.getStatusStore().isEmpty());

    // once scale status expired, throughput info will be expired in 48 hours
    jobWorkloadMonitor.cleanUp();
    Assert.assertFalse(jobWorkloadMonitor.getJobGroupWorkloadMap().isEmpty());
    testTicker.add(Duration.ofHours(49));
    autoScalar.runSample();
    jobWorkloadMonitor.cleanUp();
    Assert.assertTrue(jobWorkloadMonitor.getJobGroupWorkloadMap().isEmpty());
  }

  @Test
  public void testScaleWhenDryRun() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobWorkloadMonitor.consume(job, Workload.of(8000, 10000, 0));
      autoScalar.runSample();
    }
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2.792, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        5584, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        139600, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
    config.setDryRun(true);
    autoScalar.apply(rebalancingJobGroup, 1.6d);
    Assert.assertEquals(1.6, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertEquals(
        3200, rebalancingJobGroup.getThroughput().get().getMessagesPerSecond(), 0.001);
    Assert.assertEquals(
        80000, rebalancingJobGroup.getThroughput().get().getBytesPerSecond(), 0.001);
  }

  @Test
  public void testSnapshot() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    testTicker.add(Duration.ofSeconds(5));
    jobWorkloadMonitor.consume(job, Workload.of(4000, 10000, 0));
    MessageOrBuilder snapshot = autoScalar.snapshot();
    Assert.assertNotNull(snapshot);
    Assert.assertTrue(snapshot instanceof AutoScalarSnapshot);
    Assert.assertEquals(1, ((AutoScalarSnapshot) snapshot).getJobGroupScalarList().size());
    Assert.assertEquals(
        JobGroupKey.of(job).getGroup(),
        ((AutoScalarSnapshot) snapshot).getJobGroupScalarList().get(0).getConsumerGroup());
  }
}
