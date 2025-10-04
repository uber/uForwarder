package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class PipelineHealthManagerTest {
  private PipelineHealthManager pipelineHealthManager;

  private TestUtils.TestTicker ticker;
  private Job job;

  private PipelineHealthIssue issue1 = new PipelineHealthIssue("test-issue-1");
  private PipelineHealthIssue issue2 = new PipelineHealthIssue("test-issue-2");

  private Scope mockScope;
  private Gauge mockGauge;

  @BeforeEach
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
    Set<PipelineHealthIssue> issues = pipelineHealthManager.getPipelineHealthIssues(job);
    Assertions.assertEquals(issues, Set.of(issue1, issue2));
    pipelineHealthManager.cancel(job);
    Assertions.assertEquals(
        Collections.emptySet(), pipelineHealthManager.getPipelineHealthIssues(job));
  }

  @Test
  public void testReportIssuesPartialExpire() {
    pipelineHealthManager.reportIssue(job, issue1);
    ticker.add(Duration.ofSeconds(15));
    pipelineHealthManager.reportIssue(job, issue2);
    Set<PipelineHealthIssue> issues = pipelineHealthManager.getPipelineHealthIssues(job);
    Assertions.assertEquals(issues, Set.of(issue1, issue2));
    ticker.add(Duration.ofSeconds(20));
    issues = pipelineHealthManager.getPipelineHealthIssues(job);
    Assertions.assertEquals(issues, Set.of(issue2));
    pipelineHealthManager.cancelAll();
    Assertions.assertEquals(
        Collections.emptySet(), pipelineHealthManager.getPipelineHealthIssues(job));
  }

  @Test
  public void testGetValueAfterExpiration() {
    pipelineHealthManager.reportIssue(job, issue1);
    pipelineHealthManager.reportIssue(job, issue2);
    ticker.add(Duration.ofMinutes(1));
    Set<PipelineHealthIssue> issues = pipelineHealthManager.getPipelineHealthIssues(job);
    Assertions.assertEquals(0, issues.size());
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
    Set<PipelineHealthIssue> issues = pipelineHealthManager.getPipelineHealthIssues(job2);
    Assertions.assertEquals(0, issues.size());
  }

  @Test
  public void testPublishMetrics() {
    pipelineHealthManager.reportIssue(job, issue1);
    pipelineHealthManager.publishMetrics();
    Mockito.verify(mockScope)
        .tagged(
            Map.of(
                "kafka_cluster", "cluster",
                "kafka_group", "group",
                "kafka_topic", "topic",
                "kafka_partition", "1"));
    Mockito.verify(mockScope).tagged(Map.of("error_type", "test-issue-1"));
    // Add assertions to verify the metrics published
    Mockito.verify(mockScope, Mockito.times(1))
        .gauge(PipelineHealthManager.MetricNames.JOB_HEALTH_STATE);
    Mockito.verify(mockGauge, Mockito.times(1)).update(1);
  }
}
