package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ThroughputTrackerTest extends FievelTestBase {
  private ThroughputTracker throughputTracker;
  private TestUtils.TestTicker testTicker;
  private Job job;

  @Before
  public void setUp() {
    job =
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setTopic("topic")
                    .setCluster("cluster")
                    .setConsumerGroup("group")
                    .setPartition(1)
                    .setStartOffset(-1)
                    .setEndOffset(100)
                    .build())
            .build();
    testTicker = new TestUtils.TestTicker();
    throughputTracker = new ThroughputTracker(testTicker);
    throughputTracker.init(job);
  }

  @Test
  public void testRecordThroughput() {
    throughputTracker.record(job, 1, 10);
    testTicker.add(Duration.ofSeconds(10));
    ThroughputTracker.Throughput throughput = throughputTracker.getThroughput(job);
    Assert.assertTrue(throughput.messagePerSec > 0.1);
    Assert.assertTrue(throughput.bytesPerSec > 0.5);
  }

  @Test
  public void testRecordWithDifferentJob() {
    Job job2 = Job.newBuilder(job).build();
    throughputTracker.record(job, 1, 10);
    testTicker.add(Duration.ofSeconds(10));
    ThroughputTracker.Throughput throughput = throughputTracker.getThroughput(job2);
    Assert.assertTrue(throughput.messagePerSec > 0.1);
    Assert.assertTrue(throughput.bytesPerSec > 0.5);
  }
}
