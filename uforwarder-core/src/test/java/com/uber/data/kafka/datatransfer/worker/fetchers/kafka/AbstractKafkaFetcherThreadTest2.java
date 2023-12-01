package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.datatransfer.worker.pipelines.KafkaPipelineStateManager;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

// this test class uses a mock kafka consumer to test.
public class AbstractKafkaFetcherThreadTest2 extends FievelTestBase {
  private static final String THREAD_NAME = "AbstractKafkaFetcherThreadTest";
  private static final String GROUP = "group";
  private static final String TOPIC = "topic";
  private AbstractKafkaFetcherThread fetcherThread;
  private CheckpointManager checkpointManager;
  private ThroughputTracker throughputTracker;
  private PipelineStateManager configManager;
  private MockConsumer mockConsumer;
  private Sink processor;
  private CoreInfra infra;
  private KafkaFetcherConfiguration kafkaFetcherConfiguration;

  @Before
  public void setup() {
    mockConsumer = new MockConsumer(OffsetResetStrategy.EARLIEST);
    processor = Mockito.mock(Sink.class);
    Mockito.when(processor.isRunning()).thenReturn(true);
    kafkaFetcherConfiguration = new KafkaFetcherConfiguration();
    kafkaFetcherConfiguration.setPollTimeoutMs(10);
    Scope scope = Mockito.mock(Scope.class);
    infra = CoreInfra.NOOP;
    Counter counter = Mockito.mock(Counter.class);
    Gauge gauge = Mockito.mock(Gauge.class);
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
    configManager =
        new KafkaPipelineStateManager(
            Job.newBuilder()
                .setKafkaConsumerTask(
                    KafkaConsumerTask.newBuilder().setConsumerGroup(GROUP).setTopic(TOPIC).build())
                .build(),
            scope);
    checkpointManager = new KafkaCheckpointManager(scope);
    throughputTracker = new ThroughputTracker();
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            infra);
    fetcherThread.setPipelineStateManager(configManager);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testCommitOffsets() throws ExecutionException, InterruptedException {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    mockConsumer.assign(Collections.singleton(tp));
    for (int i = 0; i < 10; i++) {
      ConsumerRecord<String, String> consumerRecord =
          new ConsumerRecord<>(TOPIC, 0, i + 10, 1, TimestampType.CREATE_TIME, 2, 0, 4, "", "test");
      consumerRecord.serializedValueSize();
      mockConsumer.addRecord(consumerRecord);
    }

    mockConsumer.updateBeginningOffsets(ImmutableMap.of(tp, 10L));
    mockConsumer.updateEndOffsets(ImmutableMap.of(tp, 20L));

    CompletableFuture completableFutureWithException = new CompletableFuture();
    completableFutureWithException.completeExceptionally(new Exception());
    CompletableFuture completableFuture = CompletableFuture.completedFuture(15L);
    Mockito.when(processor.submit(ArgumentMatchers.any()))
        .thenReturn(completableFutureWithException)
        .thenReturn(completableFuture);

    fetcherThread.setNextStage(processor);

    Job job = createConsumerJob(1, TOPIC, 0, GROUP, 1000, 10000);
    configManager.run(job).toCompletableFuture().get();
    fetcherThread.doWork();
    CheckpointInfo checkpointInfo = checkpointManager.getCheckpointInfo(job);
    Assert.assertEquals(15, checkpointInfo.getOffsetToCommit());
    Assert.assertEquals(0, checkpointInfo.getCommittedOffset());

    Thread.sleep(1000);
    fetcherThread.doWork();
    Assert.assertEquals(15, checkpointInfo.getOffsetToCommit());
    Assert.assertEquals(15, checkpointInfo.getCommittedOffset());
  }

  private Job createConsumerJob(
      long jobID,
      String topicName,
      int partitionId,
      String consumerGroup,
      long messageRateQuota,
      long byteRateQuota) {
    return Job.newBuilder()
        .setJobId(jobID)
        .setKafkaConsumerTask(
            KafkaConsumerTask.newBuilder()
                .setTopic(topicName)
                .setPartition(partitionId)
                .setConsumerGroup(consumerGroup)
                .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST)
                .build())
        .setFlowControl(
            FlowControl.newBuilder()
                .setMessagesPerSec(messageRateQuota)
                .setBytesPerSec(byteRateQuota)
                .setMaxInflightMessages(1)
                .build())
        .build();
  }
}
