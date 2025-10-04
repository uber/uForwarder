package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PipelineHealthManagerTest extends FievelTestBase {
  private PipelineHealthManager pipelineHealthManager;

  private TestUtils.TestTicker ticker;
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
    ticker = new TestUtils.TestTicker();
    pipelineHealthManager = PipelineHealthManager.newBuilder().setTicker(ticker).build();
  }

  @Test
  public void testReportIssue() {
    pipelineHealthManager.reportIssue(job, PipelineHealthIssue.INFLIGHT_MESSAGE_LIMITED);
    pipelineHealthManager.reportIssue(job, PipelineHealthIssue.PERMISSION_DENIED);
    int value = pipelineHealthManager.getPipelineHealthStateValue(job);
    Assert.assertEquals(
        Set.of(PipelineHealthIssue.INFLIGHT_MESSAGE_LIMITED, PipelineHealthIssue.PERMISSION_DENIED),
        PipelineHealthIssue.decode(value));
  }

  @Test
  public void testReportIssuesPartialExpire() {
    pipelineHealthManager.reportIssue(job, PipelineHealthIssue.INFLIGHT_MESSAGE_LIMITED);
    ticker.add(Duration.ofSeconds(15));
    pipelineHealthManager.reportIssue(job, PipelineHealthIssue.PERMISSION_DENIED);
    int value = pipelineHealthManager.getPipelineHealthStateValue(job);
    Assert.assertEquals(
        Set.of(PipelineHealthIssue.INFLIGHT_MESSAGE_LIMITED, PipelineHealthIssue.PERMISSION_DENIED),
        PipelineHealthIssue.decode(value));
    ticker.add(Duration.ofSeconds(20));
    value = pipelineHealthManager.getPipelineHealthStateValue(job);
    Assert.assertEquals(
        Set.of(PipelineHealthIssue.PERMISSION_DENIED), PipelineHealthIssue.decode(value));
  }

  @Test
  public void testGetValueAfterExpiration() {
    pipelineHealthManager.reportIssue(job, PipelineHealthIssue.INFLIGHT_MESSAGE_LIMITED);
    pipelineHealthManager.reportIssue(job, PipelineHealthIssue.PERMISSION_DENIED);
    ticker.add(Duration.ofMinutes(1));
    int value = pipelineHealthManager.getPipelineHealthStateValue(job);
    Assert.assertEquals(0, value);
  }

  @Test
  public void testGetValueWithInvalidJob() {
    int value = pipelineHealthManager.getPipelineHealthStateValue(Mockito.mock(Job.class));
    Assert.assertEquals(0, value);
  }
}
