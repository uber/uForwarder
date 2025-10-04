package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.AbstractKafkaFetcherThread;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.CheckpointInfo;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.CheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaCheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcherConfiguration;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.SeekStartOffsetOption;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.ThroughputTracker;
import com.uber.data.kafka.datatransfer.worker.pipelines.KafkaPipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineLoadTracker;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import io.opentracing.Tracer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class DlqTopicKafkaFetcherTest extends FievelTestBase {
  private final String THREAD_NAME = "thread-name";
  private final String TOPIC = "topic";
  private final String GROUP = "group";
  private final String BOOTSTRAP_SERVERS = "localhost:9092";

  private KafkaFetcherConfiguration kafkaFetcherConfiguration;
  private PipelineStateManager pipelineStateManager;
  private CheckpointManager checkpointManager;
  private KafkaConsumer mockConsumer;
  private CoreInfra infra;
  private Sink processor;
  private AbstractKafkaFetcherThread fetcherThread;
  private PipelineLoadTracker pipelineLoadTracker;

  @Before
  public void setUp() {
    Scope scope = Mockito.mock(Scope.class);
    Tracer tracer = Mockito.mock(Tracer.class);
    DynamicConfiguration dynamicConfiguration = Mockito.mock(DynamicConfiguration.class);
    infra = CoreInfra.builder().withScope(scope).withTracer(tracer).build();
    Counter counter = Mockito.mock(Counter.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Histogram histogram = Mockito.mock(Histogram.class);
    Timer timer = Mockito.mock(Timer.class);
    Stopwatch stopwatch = Mockito.mock(Stopwatch.class);
    Mockito.when(scope.subScope(ArgumentMatchers.anyString())).thenReturn(scope);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(scope.histogram(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
        .thenReturn(histogram);
    Mockito.when(timer.start()).thenReturn(stopwatch);

    kafkaFetcherConfiguration = new KafkaFetcherConfiguration();
    checkpointManager = new KafkaCheckpointManager(scope);
    ThroughputTracker throughputTracker = new ThroughputTracker();
    mockConsumer = Mockito.mock(KafkaConsumer.class);
    processor = Mockito.mock(Sink.class);
    pipelineLoadTracker = Mockito.mock(PipelineLoadTracker.class);
    Mockito.when(processor.isRunning()).thenReturn(true);
    pipelineStateManager =
        new KafkaPipelineStateManager(
            Job.newBuilder()
                .setKafkaConsumerTask(
                    KafkaConsumerTask.newBuilder()
                        .setTopic(TOPIC)
                        .setConsumerGroup(GROUP)
                        .setPartition(0)
                        .build())
                .build(),
            pipelineLoadTracker,
            scope);
    fetcherThread =
        new DlqTopicKafkaFetcher(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            infra);
  }

  @Test
  public void testOf() throws Exception {
    DlqTopicKafkaFetcher.of(
        THREAD_NAME, BOOTSTRAP_SERVERS, GROUP, kafkaFetcherConfiguration, false, infra);
  }

  @Test
  public void testHandleEndOffsetAndDelay() throws ExecutionException, InterruptedException {
    ConsumerRecord consumerRecord = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord.offset()).thenReturn(99L);
    Job job =
        Job.newBuilder()
            .setJobId(1)
            .setFlowControl(
                FlowControl.newBuilder()
                    .setBytesPerSec(1)
                    .setMessagesPerSec(1)
                    .setMaxInflightMessages(1)
                    .build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder().setConsumerGroup(GROUP).setEndOffset(100L).build())
            .build();
    pipelineStateManager.run(job).toCompletableFuture().get();
    // offset is not bounded by the end offset, this job is assigned to this pipeline
    Assert.assertFalse(
        fetcherThread.handleEndOffsetAndDelay(
            consumerRecord, job, checkpointManager, pipelineStateManager));

    Job job2 =
        Job.newBuilder()
            .setJobId(2)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(GROUP + "1")
                    .setEndOffset(100L)
                    .build())
            .setFlowControl(FlowControl.newBuilder().setBytesPerSec(1).setMessagesPerSec(1).build())
            .build();
    // offset is not bounded by the end offset, this job is not assigned to this pipeline
    Assert.assertTrue(
        fetcherThread.handleEndOffsetAndDelay(
            consumerRecord, job2, checkpointManager, pipelineStateManager));

    Mockito.when(consumerRecord.offset()).thenReturn(100L);
    // offset is bounded by the end offset, this job is assigned to this pipeline
    Assert.assertTrue(
        fetcherThread.handleEndOffsetAndDelay(
            consumerRecord, job, checkpointManager, pipelineStateManager));
  }

  @Test
  public void testGetSeekStartOffsetOption() {
    Assert.assertEquals(
        SeekStartOffsetOption.SEEK_TO_EARLIEST_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assert.assertEquals(
        SeekStartOffsetOption.SEEK_TO_LATEST_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));

    Assert.assertEquals(
        SeekStartOffsetOption.SEEK_TO_EARLIEST_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assert.assertEquals(
        SeekStartOffsetOption.SEEK_TO_LATEST_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));

    Assert.assertEquals(
        SeekStartOffsetOption.SEEK_TO_EARLIEST_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
    Assert.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
    Assert.assertEquals(
        SeekStartOffsetOption.SEEK_TO_LATEST_OFFSET,
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

  /**
   * Master issues merge for start = 0, End = 100 Poll returns 200 messages process 0->100 and
   * discard (continue) 100 -> 200. Verify this is delivered to processor. Master issues merge for
   * 100 -> 200. Worker should seek from 200 -> 100 and read 100 -> 200. Verify this is delivered to
   * processor.
   */
  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testBoundedJobs() throws InterruptedException, ExecutionException {
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> mapCaptor =
        ArgumentCaptor.forClass(Map.class);
    Mockito.when(processor.submit(ArgumentMatchers.any()))
        .thenReturn(CompletableFuture.completedFuture(50L));
    fetcherThread.setNextStage(processor);
    fetcherThread.setPipelineStateManager(pipelineStateManager);

    TopicPartition tp = new TopicPartition(TOPIC, 0);

    ConsumerRecords consumerRecords = Mockito.mock(ConsumerRecords.class);
    Set<TopicPartition> partitions = new HashSet<>();
    partitions.add(tp);
    Mockito.when(consumerRecords.partitions()).thenReturn(partitions);

    List<ConsumerRecord> consumerRecordList = new ArrayList<>();
    for (long i = 0; i < 200; i++) {
      ConsumerRecord consumerRecord = Mockito.mock(ConsumerRecord.class);
      Mockito.when(consumerRecord.topic()).thenReturn(TOPIC);
      Mockito.when(consumerRecord.partition()).thenReturn(0);
      Mockito.when(consumerRecord.offset()).thenReturn(i);
      Mockito.when(consumerRecord.serializedValueSize()).thenReturn(1);
      consumerRecordList.add(consumerRecord);
    }
    Mockito.when(consumerRecords.records(tp)).thenReturn(consumerRecordList);

    Mockito.when(mockConsumer.beginningOffsets(Collections.singleton(tp)))
        .thenReturn(ImmutableMap.of(tp, 0L));
    Mockito.when(mockConsumer.endOffsets(Collections.singleton(tp)))
        .thenReturn(ImmutableMap.of(tp, 1000L));
    // Poll returns 200 messages
    Mockito.when(mockConsumer.poll(ArgumentMatchers.any()))
        .thenReturn(consumerRecords)
        .thenReturn(null);
    KafkaConsumerTask task =
        KafkaConsumerTask.newBuilder()
            .setTopic(TOPIC)
            .setPartition(0)
            .setConsumerGroup(GROUP)
            .build();
    Job job =
        Job.newBuilder()
            .setJobId(0)
            .setFlowControl(
                FlowControl.newBuilder()
                    .setMessagesPerSec(1000)
                    .setBytesPerSec(1000000)
                    .setMaxInflightMessages(1)
                    .build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setTopic(TOPIC)
                    .setPartition(0)
                    .setConsumerGroup(GROUP)
                    .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST)
                    .setStartOffset(0)
                    .setEndOffset(100)
                    .build())
            .build();
    // Master issues merge for start = 0, End = 100, but start will not be respected.
    pipelineStateManager.run(job).toCompletableFuture().get();
    // force to do work
    fetcherThread.doWork();
    // commit offset even if the commit offset is not moving
    Mockito.verify(mockConsumer, Mockito.times(1)).commitSync(mapCaptor.capture());
    CheckpointInfo checkpointInfo = checkpointManager.getCheckpointInfo(job);
    Assert.assertNotNull(checkpointInfo);
    // wait at most 5s
    for (int i = 0; i < 5000 && checkpointInfo.getFetchOffset() != 100L; i++) {
      Thread.sleep(1);
    }

    // process 100 -> 200. Verify this is delivered to processor.
    Mockito.verify(mockConsumer, Mockito.times(1)).poll(java.time.Duration.ofMillis(100));
    Mockito.verify(processor, Mockito.times(100)).submit(ArgumentMatchers.any());
    Assert.assertEquals(0L, checkpointInfo.getStartingOffset());
    Assert.assertEquals(50L, checkpointInfo.getOffsetToCommit());
    Assert.assertEquals(0L, checkpointInfo.getCommittedOffset());
    Assert.assertEquals(100L, checkpointInfo.getFetchOffset());

    Thread.sleep(1000);
    // force to do work
    fetcherThread.doWork();
    Mockito.verify(mockConsumer, Mockito.times(2)).commitSync(mapCaptor.capture());
    Assert.assertEquals(50L, checkpointInfo.getCommittedOffset());
    // cancel the job
    pipelineStateManager.cancel(job).toCompletableFuture().get();
    fetcherThread.doWork();

    consumerRecordList = new ArrayList<>();
    for (long i = 0; i < 200; i++) {
      ConsumerRecord consumerRecord = Mockito.mock(ConsumerRecord.class);
      Mockito.when(consumerRecord.topic()).thenReturn(TOPIC);
      Mockito.when(consumerRecord.partition()).thenReturn(0);
      Mockito.when(consumerRecord.offset()).thenReturn(i + 100);
      Mockito.when(consumerRecord.serializedValueSize()).thenReturn(1);
      consumerRecordList.add(consumerRecord);
    }
    Mockito.when(consumerRecords.records(tp)).thenReturn(consumerRecordList);
    // Poll returns 200 messages
    Mockito.when(mockConsumer.poll(ArgumentMatchers.any()))
        .thenReturn(consumerRecords)
        .thenReturn(null);
    Job newJob =
        Job.newBuilder()
            .setJobId(0)
            .setFlowControl(
                FlowControl.newBuilder()
                    .setMessagesPerSec(1000)
                    .setBytesPerSec(1000000)
                    .setMaxInflightMessages(1)
                    .build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setTopic(TOPIC)
                    .setPartition(0)
                    .setConsumerGroup(GROUP)
                    .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST)
                    .setStartOffset(100)
                    .setEndOffset(200)
                    .build())
            .build();
    // Master issues merge for start = 100, End = 200
    pipelineStateManager.run(newJob).toCompletableFuture().get();
    // force to do work
    fetcherThread.doWork();

    checkpointInfo = checkpointManager.getCheckpointInfo(newJob);
    Assert.assertNotNull(checkpointInfo);
    // wait at most 5s
    for (int i = 0; i < 5000 && checkpointInfo.getFetchOffset() != 200L; i++) {
      Thread.sleep(1);
    }
    Mockito.verify(mockConsumer, Mockito.times(1)).seek(tp, 100L);
    Assert.assertEquals(100L, checkpointInfo.getStartingOffset());
    Assert.assertEquals(200L, checkpointInfo.getFetchOffset());
    // as we always commit 50, which is < 100
    Assert.assertEquals(100L, checkpointInfo.getOffsetToCommit());
    // this is accumulated from the previous test
    Mockito.verify(processor, Mockito.times(200)).submit(ArgumentMatchers.any());
  }
}
