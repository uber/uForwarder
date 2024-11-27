package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.AbstractKafkaFetcherThread;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.CheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.DelayProcessManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaCheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaDelayProcessManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcherConfiguration;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.SeekStartOffsetOption;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.ThroughputTracker;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class RetryTopicKafkaFetcherTest extends FievelTestBase {
  private final String THREAD_NAME = "thread-name";
  private final String TOPIC = "topic";
  private final String GROUP = "group";
  private final String BOOTSTRAP_SERVERS = "localhost:9092";

  private KafkaFetcherConfiguration kafkaFetcherConfiguration;
  private PipelineStateManager pipelineStateManager;
  private CheckpointManager checkpointManager;
  private ThroughputTracker throughputTracker;
  private KafkaConsumer mockConsumer;
  private CoreInfra infra;
  private Sink processor;
  private AbstractKafkaFetcherThread fetcherThread;
  private Optional<RetryQueue> retryQueue;

  @Before
  public void setUp() {
    infra = CoreInfra.NOOP;

    kafkaFetcherConfiguration = new KafkaFetcherConfiguration();
    checkpointManager = new KafkaCheckpointManager(infra.scope());
    throughputTracker = new ThroughputTracker();
    mockConsumer = Mockito.mock(KafkaConsumer.class);
    processor = Mockito.mock(Sink.class);
    pipelineStateManager = Mockito.mock(PipelineStateManager.class);
    fetcherThread =
        new RetryTopicKafkaFetcher(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            DelayProcessManager.NOOP,
            mockConsumer,
            infra,
            retryQueue);
  }

  @Test
  public void testOf() throws Exception {
    RetryTopicKafkaFetcher.of(
        THREAD_NAME,
        BOOTSTRAP_SERVERS,
        GROUP,
        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST,
        kafkaFetcherConfiguration,
        null,
        false,
        infra);
  }

  @Test
  public void testHandleEndOffsetAndDelayWithoutDelay() throws InterruptedException {
    Assert.assertTrue(
        fetcherThread.handleEndOffsetAndDelay(
            Mockito.mock(ConsumerRecord.class),
            Mockito.mock(Job.class),
            checkpointManager,
            pipelineStateManager));
    Job job =
        Job.newBuilder()
            .setFlowControl(
                FlowControl.newBuilder()
                    .setBytesPerSec(1)
                    .setMessagesPerSec(1)
                    .setMaxInflightMessages(1)
                    .build())
            .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().setConsumerGroup(GROUP).build())
            .build();

    Mockito.when(pipelineStateManager.shouldJobBeRunning(job)).thenReturn(true);
    Assert.assertFalse(
        fetcherThread.handleEndOffsetAndDelay(
            Mockito.mock(ConsumerRecord.class), job, checkpointManager, pipelineStateManager));
  }

  @Test
  public void testHandleEndOffsetAndDelayWithDelay()
      throws InterruptedException, ExecutionException {
    int processDelayMs = 100;
    Optional<RetryQueue> retryQueue =
        Optional.of(RetryQueue.newBuilder().setProcessingDelayMs(processDelayMs).build());
    Job job =
        Job.newBuilder()
            .setFlowControl(
                FlowControl.newBuilder()
                    .setBytesPerSec(1)
                    .setMessagesPerSec(1)
                    .setMaxInflightMessages(1)
                    .build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(GROUP)
                    .setProcessingDelayMs(processDelayMs)
                    .build())
            .setRetryConfig(RetryConfig.newBuilder().addRetryQueues(retryQueue.get()).build())
            .build();

    DelayProcessManager<byte[], byte[]> delayProcessManager =
        new KafkaDelayProcessManager<>(infra.scope(), GROUP, processDelayMs, mockConsumer);
    fetcherThread =
        new RetryTopicKafkaFetcher(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            delayProcessManager,
            mockConsumer,
            infra,
            retryQueue);

    ConsumerRecord consumerRecord = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord.timestamp()).thenReturn(System.currentTimeMillis());

    Mockito.when(pipelineStateManager.shouldJobBeRunning(job)).thenReturn(true);
    Assert.assertFalse(
        fetcherThread.handleEndOffsetAndDelay(
            consumerRecord, job, checkpointManager, pipelineStateManager));

    Mockito.when(pipelineStateManager.shouldJobBeRunning(job)).thenReturn(true).thenReturn(false);
    Mockito.when(consumerRecord.timestamp()).thenReturn(System.currentTimeMillis());
    Mockito.verify(pipelineStateManager, Mockito.times(1)).shouldJobBeRunning(job);
    Assert.assertFalse(
        fetcherThread.handleEndOffsetAndDelay(
            consumerRecord, job, checkpointManager, pipelineStateManager));
  }

  @Test
  public void testRetryProcessingDelayMSNotFallbackToOriginalTopicDelayMs()
      throws InterruptedException, ExecutionException {
    Job job =
        Job.newBuilder()
            .setFlowControl(
                FlowControl.newBuilder()
                    .setBytesPerSec(1)
                    .setMessagesPerSec(1)
                    .setMaxInflightMessages(1)
                    .build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(GROUP)
                    .setProcessingDelayMs(100)
                    .build())
            .build();

    // No retry queue config but has ProcessingDelayMs in KafkaConsumerTask.
    fetcherThread =
        new RetryTopicKafkaFetcher(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            DelayProcessManager.NOOP,
            mockConsumer,
            infra,
            retryQueue);

    ConsumerRecord consumerRecord = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord.timestamp()).thenReturn(System.currentTimeMillis());

    Mockito.when(pipelineStateManager.shouldJobBeRunning(job)).thenReturn(true);
    Assert.assertFalse(
        fetcherThread.handleEndOffsetAndDelay(
            consumerRecord, job, checkpointManager, pipelineStateManager));

    Mockito.when(pipelineStateManager.shouldJobBeRunning(job)).thenReturn(true).thenReturn(false);
    Mockito.when(consumerRecord.timestamp()).thenReturn(System.currentTimeMillis());

    // since ProcessingDelayMs is -1, so it won't go into the loop and call shouldJobBeRunning
    // again.
    Mockito.verify(pipelineStateManager, Mockito.times(1)).shouldJobBeRunning(job);
  }

  @Test
  public void testGetSeekStartOffsetOption() {
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));

    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));

    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));

    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, null, null, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, null, null, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, null, null, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
  }
}
