package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PipelineHealthManagerTest extends FievelTestBase {
  private PipelineHealthManager pipelineHealthManager;

  private TestUtils.TestTicker ticker;
  private Job job;

  private PipelineHealthIssue issue1 = new PipelineHealthIssue(0);
  private PipelineHealthIssue issue2 = new PipelineHealthIssue(1);

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
    ticker = new TestUtils.TestTicker();
    pipelineHealthManager = PipelineHealthManager.newBuilder().setTicker(ticker).build(job);
    pipelineHealthManager.init(job);
  }

  @Test
  public void testReportIssue() {
    pipelineHealthManager.reportIssue(job, issue1);
    pipelineHealthManager.reportIssue(job, issue2);
    int value = pipelineHealthManager.getPipelineHealthStateValue(job);
    Assert.assertEquals(value, issue1.getValue() + issue2.getValue());
    pipelineHealthManager.cancel(job);
    Assert.assertEquals(0, pipelineHealthManager.getPipelineHealthStateValue(job));
  }

  @Test
  public void testReportIssuesPartialExpire() {
    pipelineHealthManager.reportIssue(job, issue1);
    ticker.add(Duration.ofSeconds(15));
    pipelineHealthManager.reportIssue(job, issue2);
    int value = pipelineHealthManager.getPipelineHealthStateValue(job);
    Assert.assertEquals(value, issue1.getValue() + issue2.getValue());
    ticker.add(Duration.ofSeconds(20));
    value = pipelineHealthManager.getPipelineHealthStateValue(job);
    Assert.assertEquals(value, issue2.getValue());
    pipelineHealthManager.cancelAll();
    Assert.assertEquals(0, pipelineHealthManager.getPipelineHealthStateValue(job));
  }

  @Test
  public void testGetValueAfterExpiration() {
    pipelineHealthManager.reportIssue(job, issue1);
    pipelineHealthManager.reportIssue(job, issue2);
    ticker.add(Duration.ofMinutes(1));
    int value = pipelineHealthManager.getPipelineHealthStateValue(job);
    Assert.assertEquals(0, value);
  }

  @Test
  public void testGetValueWithInvalidJob() {
    Job job2 =
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setTopic("topic")
                    .setCluster("cluster")
                    .setConsumerGroup("group")
                    .setPartition(2)
                    .setStartOffset(-1)
                    .setEndOffset(100)
                    .build())
            .build();
    int value = pipelineHealthManager.getPipelineHealthStateValue(job2);
    Assert.assertEquals(0, value);
  }
}
