package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class PipelineHealthManagerTest extends FievelTestBase {
  private PipelineHealthManager pipelineHealthManager;

  private TestUtils.TestTicker ticker;
  private Job job;

  private PipelineHealthIssue issue1 = new PipelineHealthIssue(0);
  private PipelineHealthIssue issue2 = new PipelineHealthIssue(1);

  private Scope mockScope;
  private Gauge mockGauge;

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
    mockScope = Mockito.mock(Scope.class);
    mockGauge = Mockito.mock(Gauge.class);
    Mockito.when(mockScope.tagged(ArgumentMatchers.anyMap())).thenReturn(mockScope);
    // Mockito.doReturn(mockScope).when(mockScope).tagged(ArgumentMatchers.anyMap());
    Mockito.when(mockScope.gauge(ArgumentMatchers.anyString())).thenReturn(mockGauge);
    pipelineHealthManager =
        PipelineHealthManager.newBuilder().setTicker(ticker).setScope(mockScope).build(job);
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

  @Test
  public void testPublishMetrics() {
    pipelineHealthManager.reportIssue(job, issue1);
    pipelineHealthManager.publishMetrics();
    Mockito.verify(mockScope, Mockito.times(1))
        .tagged(
            Mockito.argThat(
                tags ->
                    tags.get("kafka_cluster").equals("cluster")
                        && tags.get("kafka_group").equals("group")
                        && tags.get("kafka_topic").equals("topic")
                        && tags.get("kafka_partition").equals("1")));
    // Add assertions to verify the metrics published
    Mockito.verify(mockScope, Mockito.times(1))
        .gauge(PipelineHealthManager.MetricNames.JOB_HEALTH_STATE);
    Mockito.verify(mockGauge, Mockito.times(1)).update(issue1.getValue());
  }
}
