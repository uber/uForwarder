package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.MiscConfig;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class KafkaPipelineStateManagerTest extends FievelTestBase {

  private final String TOPIC = "topic";
  private final String GROUP = "group";
  private final String GROUP2 = "group2";
  private final String CONSUMER_SERVICE_NAME = "consumer-service";

  private PipelineStateManager pipelineStateManager;
  private Scope scope;
  private Gauge gauge;

  @Before
  public void setUp() {
    scope = Mockito.mock(Scope.class);
    Counter counter = Mockito.mock(Counter.class);
    gauge = Mockito.mock(Gauge.class);
    Timer timer = Mockito.mock(Timer.class);
    Histogram histogram = Mockito.mock(Histogram.class);
    Stopwatch stopwatch = Mockito.mock(Stopwatch.class);
    Mockito.when(scope.subScope(ArgumentMatchers.anyString())).thenReturn(scope);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(scope.histogram(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
        .thenReturn(histogram);
    Mockito.when(timer.start()).thenReturn(stopwatch);

    pipelineStateManager =
        new KafkaPipelineStateManager(
            Job.newBuilder()
                .setKafkaConsumerTask(
                    KafkaConsumerTask.newBuilder().setConsumerGroup(GROUP).setTopic(TOPIC).build())
                .setMiscConfig(
                    MiscConfig.newBuilder().setOwnerServiceName(CONSUMER_SERVICE_NAME).build())
                .build(),
            scope);
  }

  @Test
  public void testUpdateActualRunningJobStatus() {
    ImmutableList<JobStatus> jobStatusList = ImmutableList.of(JobStatus.newBuilder().build());
    pipelineStateManager.updateActualRunningJobStatus(jobStatusList);
    Assert.assertEquals(jobStatusList, pipelineStateManager.getJobStatus());
  }

  @Test
  public void testGet() {
    Job jobDefinitionTemplate =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder().setConsumerGroup(GROUP).setTopic(TOPIC).build())
            .setMiscConfig(
                MiscConfig.newBuilder().setOwnerServiceName(CONSUMER_SERVICE_NAME).build())
            .build();
    Assert.assertEquals(jobDefinitionTemplate, pipelineStateManager.getJobTemplate());
    validateMap(0);
    Assert.assertEquals(
        FlowControl.newBuilder()
            .setMessagesPerSec(1.0)
            .setBytesPerSec(Double.MAX_VALUE)
            .setMaxInflightMessages(1.0)
            .buildPartial(),
        pipelineStateManager.getFlowControl());
  }

  @Test
  public void testShouldJobBeRunning() throws ExecutionException, InterruptedException {
    Assert.assertFalse(pipelineStateManager.shouldJobBeRunning(Job.getDefaultInstance()));
    pipelineStateManager
        .run(createConsumerJob(1, TOPIC, 0, GROUP, 1000, 1000, 1))
        .toCompletableFuture()
        .get();
    Assert.assertTrue(
        pipelineStateManager.shouldJobBeRunning(
            createConsumerJob(1, TOPIC, 0, GROUP, 1000, 1000, 1)));
  }

  @Test
  public void testGetExpectedJob() throws ExecutionException, InterruptedException {
    Assert.assertFalse(pipelineStateManager.getExpectedJob(1).isPresent());
    pipelineStateManager
        .run(createConsumerJob(1, TOPIC, 0, GROUP, 1000, 1000, 1))
        .toCompletableFuture()
        .get();
    Assert.assertEquals(
        createConsumerJob(1, TOPIC, 0, GROUP, 1000, 1000, 1),
        pipelineStateManager.getExpectedJob(1).get());
  }

  @Test
  public void testClear() throws ExecutionException, InterruptedException {
    Job job = createConsumerJob(1, TOPIC, 0, GROUP, 1000, 1000, 1);
    pipelineStateManager.run(job).toCompletableFuture().get();
    validateMap(1);
    Assert.assertEquals(
        FlowControl.newBuilder()
            .setMessagesPerSec(1000)
            .setBytesPerSec(1000)
            .setMaxInflightMessages(1)
            .build(),
        pipelineStateManager.getFlowControl());
    pipelineStateManager.updateActualRunningJobStatus(
        ImmutableList.of(JobStatus.getDefaultInstance()));
    Assert.assertEquals(1, pipelineStateManager.getJobStatus().size());
    pipelineStateManager.clear();
    validateMap(0);
    Assert.assertEquals(
        FlowControl.newBuilder()
            .setMessagesPerSec(1.0)
            .setBytesPerSec(Double.MAX_VALUE)
            .setMaxInflightMessages(1.0)
            .build(),
        pipelineStateManager.getFlowControl());
    Assert.assertEquals(0, pipelineStateManager.getJobStatus().size());
  }

  @Test
  public void testRun() throws ExecutionException, InterruptedException {
    Job job0 = createConsumerJob(1, TOPIC, 0, "", 1000, 1000, 1);
    try {
      pipelineStateManager.run(job0).toCompletableFuture().get();
      Assert.fail("run should failed because of it belong to a different consumer group");
    } catch (ExecutionException e) {
      Assert.assertEquals(
          "java.lang.RuntimeException: consumer group for task is  doesn't match "
              + "consumer group for fetcher thread group",
          e.getMessage());
    }
    validateMap(0);
    // run with a different group
    Job job1 = createConsumerJob(1, TOPIC, 0, GROUP2, 1000, 1000, 1);
    try {
      pipelineStateManager.run(job1).toCompletableFuture().get();
      Assert.fail("run should failed because of it belong to a different consumer group");
    } catch (ExecutionException e) {
      Assert.assertEquals(
          "java.lang.RuntimeException: consumer group for task is group2 doesn't match "
              + "consumer group for fetcher thread group",
          e.getMessage());
    }
    validateMap(0);

    // run with non-positive quota
    Job job2 = createConsumerJob(2, TOPIC, 0, GROUP, 0, 1000, 1);
    ExecutionException e3 =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> {
              pipelineStateManager.run(job2).toCompletableFuture().get();
            });
    Assert.assertEquals(
        "java.lang.RuntimeException: flow control for job 2 is not positive", e3.getMessage());
    validateMap(0);

    // run with non-positive quota
    Job job3 = createConsumerJob(3, TOPIC, 0, GROUP, 1000, -1, 1);
    ExecutionException e0 =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> {
              pipelineStateManager.run(job3).toCompletableFuture().get();
            });
    Assert.assertEquals(
        "java.lang.RuntimeException: flow control for job 3 is not positive", e0.getMessage());
    validateMap(0);

    // a valid job
    Job job4 = createConsumerJob(4, TOPIC, 0, GROUP, 1000, 1000, 1);
    pipelineStateManager.run(job4).toCompletableFuture().get();
    validateMap(1);

    // add the same job
    Job job5 = createConsumerJob(4, TOPIC, 0, GROUP, 1000, 1000, 1);
    pipelineStateManager.run(job5).toCompletableFuture().get();
    validateMap(1);

    // add a different job
    Job job6 = createConsumerJob(5, TOPIC, 1, GROUP, 1000, 1000, 1);
    pipelineStateManager.run(job6).toCompletableFuture().get();
    validateMap(2);
  }

  @Test
  public void testUpdate() throws ExecutionException, InterruptedException {
    // update with a different group
    Job job1 = createConsumerJob(1, TOPIC, 0, GROUP2, 1000, 1000, 1);
    try {
      pipelineStateManager.update(job1).toCompletableFuture().get();
      Assert.fail("update should failed because of it belong to a different consumer group");
    } catch (ExecutionException e) {
      Assert.assertEquals(
          "java.lang.RuntimeException: consumer group for task is group2 doesn't match "
              + "consumer group for fetcher thread group",
          e.getMessage());
    }

    // update with non-positive quota
    Job job2 = createConsumerJob(2, TOPIC, 0, GROUP, 0, 1000, 1);
    ExecutionException e2 =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> {
              pipelineStateManager.update(job2).toCompletableFuture().get();
            });
    Assert.assertEquals(
        "java.lang.RuntimeException: flow control for job 2 is not positive", e2.getMessage());
    validateMap(0);

    // update with non-positive quota
    Job job3 = createConsumerJob(3, TOPIC, 0, GROUP, 1000, -1, 1);
    ExecutionException e1 =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> {
              pipelineStateManager.update(job3).toCompletableFuture().get();
            });
    Assert.assertEquals(
        "java.lang.RuntimeException: flow control for job 3 is not positive", e1.getMessage());
    validateMap(0);

    // a valid job
    Job job4 = createConsumerJob(4, TOPIC, 0, GROUP, 1000, 1000, 1);
    pipelineStateManager.update(job4).toCompletableFuture().get();
    validateMap(0);
    pipelineStateManager.run(job4).toCompletableFuture().get();
    validateMap(1);
    pipelineStateManager.update(job4).toCompletableFuture().get();
    validateMap(1);
    Assert.assertEquals(
        1000,
        ((Job) pipelineStateManager.getExpectedRunningJobMap().values().toArray()[0])
            .getFlowControl()
            .getMessagesPerSec(),
        0);

    // add the same job
    Job job5 = createConsumerJob(4, TOPIC, 0, GROUP, 100, 1000, 1);
    pipelineStateManager.update(job5).toCompletableFuture().get();
    validateMap(1);
    Assert.assertEquals(
        100,
        ((Job) pipelineStateManager.getExpectedRunningJobMap().values().toArray()[0])
            .getFlowControl()
            .getMessagesPerSec(),
        0);

    // add a different job
    Job job6 = createConsumerJob(5, TOPIC, 1, GROUP, 1000, 1000, 1);
    pipelineStateManager.update(job6).toCompletableFuture().get();
    validateMap(1);
    pipelineStateManager.run(job6).toCompletableFuture().get();
    validateMap(2);
    pipelineStateManager.update(job6).toCompletableFuture().get();
    validateMap(2);
  }

  @Test
  public void testCancel() throws ExecutionException, InterruptedException {
    // cancel with a different group
    Job job1 = createConsumerJob(1, TOPIC, 0, GROUP2, 1000, 1000, 1);
    try {
      pipelineStateManager.cancel(job1).toCompletableFuture().get();
      Assert.fail("cancel should failed because of it belong to a different consumer group");
    } catch (ExecutionException e) {
      Assert.assertEquals(
          "java.lang.RuntimeException: consumer group for task is group2 doesn't match "
              + "consumer group for fetcher thread group",
          e.getMessage());
    }

    // cancel a valid job
    Job job2 = createConsumerJob(2, TOPIC, 0, GROUP, 1000, 1000, 1);
    pipelineStateManager.cancel(job2).toCompletableFuture().get();
    validateMap(0);

    pipelineStateManager.run(job2).toCompletableFuture().get();
    validateMap(1);
    pipelineStateManager.cancel(job2).toCompletableFuture().get();
    validateMap(0);
  }

  @Test
  public void testRunAndCancelAll() throws ExecutionException, InterruptedException {
    testRun();
    pipelineStateManager.cancelAll().toCompletableFuture().get();
    validateMap(0);
  }

  @Test
  public void testRunAndCancel() throws ExecutionException, InterruptedException {
    testRun();
    // cancel a valid job
    Job job1 = createConsumerJob(4, TOPIC, 0, GROUP, 1000, 1000, 1);
    pipelineStateManager.cancel(job1).toCompletableFuture().get();
    validateMap(1);

    // cancel the same valid job
    Job job2 = createConsumerJob(4, TOPIC, 0, GROUP, 1000, 1000, 1);
    pipelineStateManager.cancel(job2).toCompletableFuture().get();
    validateMap(1);

    // cancel a different valid job
    Job job3 = createConsumerJob(5, TOPIC, 1, GROUP, 1000, 1000, 1);
    pipelineStateManager.cancel(job3).toCompletableFuture().get();
    validateMap(0);
  }

  @Test
  public void testHandleFlowChange() throws ExecutionException, InterruptedException {
    Job job1 = createConsumerJob(0, TOPIC, 0, GROUP, 2000, 1000, 1);
    Job job2 = createConsumerJob(1, TOPIC, 1, GROUP, 200, 100, 2);
    pipelineStateManager.run(job1).toCompletableFuture().get();
    pipelineStateManager.run(job2).toCompletableFuture().get();
    Assert.assertEquals(1100, (int) pipelineStateManager.getFlowControl().getBytesPerSec());
    Assert.assertEquals(2200, (int) pipelineStateManager.getFlowControl().getMessagesPerSec());
    Assert.assertEquals(3, (int) pipelineStateManager.getFlowControl().getMaxInflightMessages());

    pipelineStateManager.run(job1).toCompletableFuture().get();
    Assert.assertEquals(1100, (int) pipelineStateManager.getFlowControl().getBytesPerSec());
    Assert.assertEquals(2200, (int) pipelineStateManager.getFlowControl().getMessagesPerSec());
    Assert.assertEquals(3, (int) pipelineStateManager.getFlowControl().getMaxInflightMessages());

    pipelineStateManager.cancel(job1).toCompletableFuture().get();
    Assert.assertEquals(100, (int) pipelineStateManager.getFlowControl().getBytesPerSec());
    Assert.assertEquals(200, (int) pipelineStateManager.getFlowControl().getMessagesPerSec());
    Assert.assertEquals(2, (int) pipelineStateManager.getFlowControl().getMaxInflightMessages());

    pipelineStateManager.cancelAll().toCompletableFuture().get();
    Assert.assertEquals(
        Double.MAX_VALUE, pipelineStateManager.getFlowControl().getBytesPerSec(), 0.000001);
    Assert.assertEquals(1, (int) pipelineStateManager.getFlowControl().getMessagesPerSec());
    Assert.assertEquals(1, (int) pipelineStateManager.getFlowControl().getMaxInflightMessages());
  }

  @Test
  public void testPublishMetrics() throws ExecutionException, InterruptedException {
    Job job = createConsumerJob(0, TOPIC, 0, GROUP, 2000, 1000, 1);
    pipelineStateManager.run(job).toCompletableFuture().get();
    pipelineStateManager.publishMetrics();
    ArgumentCaptor<Double> valueCaptor = ArgumentCaptor.forClass(Double.class);
    Mockito.verify(gauge, Mockito.times(3)).update(valueCaptor.capture());
    Assert.assertArrayEquals(
        new Double[] {
          job.getFlowControl().getMessagesPerSec(),
          job.getFlowControl().getBytesPerSec(),
          job.getFlowControl().getMaxInflightMessages()
        },
        valueCaptor.getAllValues().toArray(new Double[] {}));
  }

  @Test
  public void testReportIssue() {
    Job job = createConsumerJob(0, TOPIC, 0, GROUP, 2000, 1000, 1);
    pipelineStateManager.reportIssue(job, new PipelineHealthIssue(0));
  }

  private Job createConsumerJob(
      long jobID,
      String topicName,
      int partitionId,
      String consumerGroup,
      long messageRateQuota,
      long byteRateQuota,
      long maxInflightQuota) {
    return Job.newBuilder()
        .setJobId(jobID)
        .setKafkaConsumerTask(
            KafkaConsumerTask.newBuilder()
                .setTopic(topicName)
                .setPartition(partitionId)
                .setConsumerGroup(consumerGroup)
                .build())
        .setFlowControl(
            FlowControl.newBuilder()
                .setMessagesPerSec(messageRateQuota)
                .setBytesPerSec(byteRateQuota)
                .setMaxInflightMessages(maxInflightQuota)
                .build())
        .setMiscConfig(MiscConfig.newBuilder().setOwnerServiceName(CONSUMER_SERVICE_NAME).build())
        .build();
  }

  private void validateMap(int jobRunningMapSize) {
    Assert.assertEquals(jobRunningMapSize, pipelineStateManager.getExpectedRunningJobMap().size());
  }
}
