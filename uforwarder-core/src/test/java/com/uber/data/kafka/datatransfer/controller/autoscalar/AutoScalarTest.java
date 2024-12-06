package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.collect.ImmutableMap;
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
  private JobThroughputMonitor jobThroughputMonitor;
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
    config.setJobStatusTTL(Duration.ofDays(2));
    config.setUpScaleMaxFactor(1.4);
    config.setUpScaleMinFactor(1.1);
    config.setDownScaleMaxFactor(0.9);
    config.setDownScaleMinFactor(0.8);
    jobThroughputMonitor = new JobThroughputMonitor(config, testTicker, new NoopScope());
    leaderSelector = Mockito.mock(LeaderSelector.class);
    Mockito.when(leaderSelector.isLeader()).thenReturn(true);
    autoScalar =
        new AutoScalar(config, jobThroughputMonitor, testTicker, new NoopScope(), leaderSelector);
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
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobThroughputMonitor.consume(job, i < 28 ? 4000 : 8000, 10000);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2.792, rebalancingJobGroup.getScale().get(), 0.001);
  }

  @Test
  public void testScaleUpBelowMinFactor() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobThroughputMonitor.consume(job, 4100, 10000);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
  }

  @Test
  public void testScaleUp20Percent() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobThroughputMonitor.consume(job, i < 28 ? 4000 : 4800, 10000);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2.4, rebalancingJobGroup.getScale().get(), 0.001);
  }

  @Test
  public void testScaleDownBelowMin() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      jobThroughputMonitor.consume(job, i < samples - 10 ? 1000 : 4000, 800);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(1.6, rebalancingJobGroup.getScale().get(), 0.001);
  }

  @Test
  public void testSmallScaleNotScaleDown() {
    rebalancingJobGroup.updateScale(1e-8);
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(1e-8, rebalancingJobGroup.getScale().get(), 1e-9);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      jobThroughputMonitor.consume(job, 1e-9, 1e-9);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(1e-8, rebalancingJobGroup.getScale().get(), 1e-9);
  }

  @Test
  public void testScaleDownAboveMax() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      jobThroughputMonitor.consume(job, 3800, 800);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
  }

  @Test
  public void testScaleDown15Percent() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      jobThroughputMonitor.consume(job, 3400, 800);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(1.7, rebalancingJobGroup.getScale().get(), 0.01);
  }

  @Test
  public void testHibernatingAndBootstrap() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    int samples = 3 * 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofMinutes(1));
      jobThroughputMonitor.consume(job, 0, 0);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertTrue(0.0 == rebalancingJobGroup.getScale().get());
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobThroughputMonitor.consume(job, 1000, 10000);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(0.5, rebalancingJobGroup.getScale().get(), 0.0001);
  }

  @Test
  public void testHibernating() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    int samples = 3 * 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofMinutes(1));
      jobThroughputMonitor.consume(job, 0, 0);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertTrue(0.0 == rebalancingJobGroup.getScale().get());
  }

  @Test
  public void testHibernatingSmallScale() {
    rebalancingJobGroup.updateScale(10e-8);
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(10e-8, rebalancingJobGroup.getScale().get(), 10e-10);
    int samples = 3 * 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofMinutes(1));
      jobThroughputMonitor.consume(job, 0, 0);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertTrue(0.0 == rebalancingJobGroup.getScale().get());
  }

  @Test
  public void testBootstrap() {
    rebalancingJobGroup.updateScale(0.0d);
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertTrue(0.0 == rebalancingJobGroup.getScale().get());
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobThroughputMonitor.consume(job, 1000, 10000);
      autoScalar.runSample();
      autoScalar.apply(rebalancingJobGroup, 0.0d);
    }
    Assert.assertEquals(0.5, rebalancingJobGroup.getScale().get(), 0.0001);
  }

  @Test
  public void testStopReportThroughput() {
    // stop report throughout stops scaling
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    jobThroughputMonitor.consume(job, 3400, 800);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      // no throughput reported
      autoScalar.runSample();
    }
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2.0, rebalancingJobGroup.getScale().get(), 0.01);
  }

  @Test
  public void testUpdateQuotaResetScale() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);

    testTicker.add(Duration.ofSeconds(60));
    jobThroughputMonitor.consume(job, 3400, 800);
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
  }

  @Test
  public void testCleanupNonRunningJobs() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    autoScalar.runSample();
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    Assert.assertFalse(jobThroughputMonitor.getJobGroupThroughputMap().isEmpty());
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
    jobThroughputMonitor.cleanUp();
    Assert.assertFalse(jobThroughputMonitor.getJobGroupThroughputMap().isEmpty());
    testTicker.add(Duration.ofHours(49));
    autoScalar.runSample();
    jobThroughputMonitor.cleanUp();
    Assert.assertTrue(jobThroughputMonitor.getJobGroupThroughputMap().isEmpty());
  }

  @Test
  public void testScaleWhenDryRun() {
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2, rebalancingJobGroup.getScale().get(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      jobThroughputMonitor.consume(job, 8000, 10000);
      autoScalar.runSample();
    }
    autoScalar.apply(rebalancingJobGroup, 0.0d);
    Assert.assertEquals(2.792, rebalancingJobGroup.getScale().get(), 0.001);
    config.setDryRun(true);
    autoScalar.apply(rebalancingJobGroup, 1.6d);
    Assert.assertEquals(1.6, rebalancingJobGroup.getScale().get(), 0.001);
  }
}
