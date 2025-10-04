package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.AbstractKafkaFetcherThread;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.CheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.DelayProcessManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcherConfiguration;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.SeekStartOffsetOption;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.ThroughputTracker;
import com.uber.data.kafka.datatransfer.worker.pipelines.KafkaPipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineLoadTracker;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import io.opentracing.Tracer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class OriginalTopicKafkaFetcherTest {
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
  private AbstractKafkaFetcherThread fetcherThread;

  @BeforeEach
  public void setUp() {
    Scope scope = Mockito.mock(Scope.class);
    Tracer tracer = Mockito.mock(Tracer.class);
    DynamicConfiguration dynamicConfiguration = Mockito.mock(DynamicConfiguration.class);
    infra = CoreInfra.builder().withScope(scope).withTracer(tracer).build();
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

    kafkaFetcherConfiguration = new KafkaFetcherConfiguration();
    checkpointManager = Mockito.mock(CheckpointManager.class);
    throughputTracker = Mockito.mock(ThroughputTracker.class);
    mockConsumer = Mockito.mock(KafkaConsumer.class);
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
            PipelineLoadTracker.NOOP,
            scope);
    fetcherThread =
        new OriginalTopicKafkaFetcher(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            DelayProcessManager.NOOP,
            mockConsumer,
            infra);
    fetcherThread.setPipelineStateManager(pipelineStateManager);
  }

  @Test
  public void testOf() throws Exception {
    OriginalTopicKafkaFetcher.of(
        THREAD_NAME,
        BOOTSTRAP_SERVERS,
        GROUP,
        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST,
        IsolationLevel.ISOLATION_LEVEL_UNSET,
        -1,
        kafkaFetcherConfiguration,
        false,
        infra);
  }

  @Test
  public void testHandleEndOffsetAndDelay() throws ExecutionException, InterruptedException {
    Assertions.assertTrue(
        fetcherThread.handleEndOffsetAndDelay(
            Mockito.mock(ConsumerRecord.class),
            Mockito.mock(Job.class),
            checkpointManager,
            pipelineStateManager));
    Job job =
        Job.newBuilder()
            .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().setConsumerGroup(GROUP).build())
            .setFlowControl(
                FlowControl.newBuilder()
                    .setBytesPerSec(1)
                    .setMessagesPerSec(1)
                    .setMaxInflightMessages(1)
                    .build())
            .build();
    pipelineStateManager.run(job).toCompletableFuture().get();
    Assertions.assertFalse(
        fetcherThread.handleEndOffsetAndDelay(
            Mockito.mock(ConsumerRecord.class), job, checkpointManager, pipelineStateManager));
  }

  @Test
  public void testGetStartOffsetToSeek() {
    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));

    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_EARLIEST_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_EARLIEST_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));

    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_LATEST_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_LATEST_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));

    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            1, null, null, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            1, null, null, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET,
        fetcherThread.getSeekStartOffsetOption(
            1, null, null, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
  }

  @Test
  public void testSeekStartOffsetIgnore() {
    TopicPartition tp1 = new TopicPartition(TOPIC, 0);
    TopicPartition tp2 = new TopicPartition(TOPIC, 1);
    TopicPartition tp3 = new TopicPartition(TOPIC, 2);
    TopicPartition tp4 = new TopicPartition(TOPIC, 3);

    Job job1 = createConsumerJob(0, TOPIC, 0, GROUP, 2000, 1000);
    Job job2 = createConsumerJob(1, TOPIC, 1, GROUP, 200, 100);
    Job job3 = createConsumerJob(2, TOPIC, 2, GROUP, 200, 100);

    Map<TopicPartition, Job> topicPartitionJobMap =
        ImmutableMap.of(tp1, job1, tp2, job2, tp3, job3);
    // an empty seekOffsetMap
    fetcherThread.seekStartOffset(ImmutableMap.of(), topicPartitionJobMap);

    // tp3 has a negative offset which should be ignored; tp4 is not in topicPartitionJobMap
    Map<TopicPartition, Long> immutableSeekOffsetMap =
        ImmutableMap.of(tp1, 1000L, tp3, -1L, tp4, 1000L);
    fetcherThread.seekStartOffset(immutableSeekOffsetMap, topicPartitionJobMap);
    // only job1 was seeked
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job1, 1000L);
    Mockito.verify(checkpointManager, Mockito.times(1))
        .setFetchOffset(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
  }

  @Test
  public void testSeekStartOffsetSeek() {
    TopicPartition tp1 = new TopicPartition(TOPIC, 0);
    TopicPartition tp2 = new TopicPartition(TOPIC, 1);

    Job job1 = createConsumerJob(0, TOPIC, 0, GROUP, 2000, 1000);
    Job job2 = createConsumerJob(1, TOPIC, 1, GROUP, 200, 100);
    Map<TopicPartition, Job> topicPartitionJobMap = ImmutableMap.of(tp1, job1, tp2, job2);
    Map<TopicPartition, Long> immutableSeekOffsetMap = ImmutableMap.of(tp1, 1000L);

    // case 1: offset is between start offset and end offset
    Mockito.when(mockConsumer.beginningOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp1, 100L));
    Mockito.when(mockConsumer.endOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp1, 2000L));
    Map<TopicPartition, Long> seekOffsetMap = new HashMap<>(immutableSeekOffsetMap);
    fetcherThread.seekStartOffset(seekOffsetMap, topicPartitionJobMap);
    Mockito.verify(mockConsumer, Mockito.times(1))
        .seek(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job1, 1000L);

    // case 2: offset is too small but auto offset reset policy is invalid
    Mockito.when(mockConsumer.beginningOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp1, 1500L));
    seekOffsetMap = new HashMap<>(immutableSeekOffsetMap);
    fetcherThread.seekStartOffset(seekOffsetMap, topicPartitionJobMap);
    // the invocation times are accumulated
    Mockito.verify(mockConsumer, Mockito.times(2))
        .seek(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
    Mockito.verify(checkpointManager, Mockito.times(2)).setFetchOffset(job1, 1000L);

    // case 3: offset is too large but auto offset reset policy is invalid
    Mockito.when(mockConsumer.beginningOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp1, 100L));
    Mockito.when(mockConsumer.endOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp1, 500L));
    seekOffsetMap = new HashMap<>(immutableSeekOffsetMap);
    fetcherThread.seekStartOffset(seekOffsetMap, topicPartitionJobMap);
    // the invocation times are accumulated
    Mockito.verify(mockConsumer, Mockito.times(3))
        .seek(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
    Mockito.verify(checkpointManager, Mockito.times(3)).setFetchOffset(job1, 1000L);

    Mockito.verify(mockConsumer, Mockito.never()).seekToBeginning(ArgumentMatchers.anyCollection());
    Mockito.verify(mockConsumer, Mockito.never()).seekToEnd(ArgumentMatchers.anyCollection());
  }

  @Test
  public void testSeekStartOffsetSeekToBeginning() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Job job =
        Job.newBuilder()
            .setJobId(0)
            .setFlowControl(
                FlowControl.newBuilder().setMessagesPerSec(1000).setBytesPerSec(1000).build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setTopic(TOPIC)
                    .setPartition(0)
                    .setConsumerGroup(GROUP)
                    .setAutoOffsetResetPolicy(
                        AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST)
                    .build())
            .build();

    Map<TopicPartition, Job> topicPartitionJobMap = ImmutableMap.of(tp, job);
    Map<TopicPartition, Long> immutableSeekOffsetMap = ImmutableMap.of(tp, 1000L);

    Mockito.when(mockConsumer.beginningOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp, 1500L));
    Mockito.when(mockConsumer.endOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp, 2000L));
    Map<TopicPartition, Long> seekOffsetMap = new HashMap<>(immutableSeekOffsetMap);
    fetcherThread.seekStartOffset(seekOffsetMap, topicPartitionJobMap);
    Mockito.verify(mockConsumer, Mockito.times(1)).seekToBeginning(ArgumentMatchers.any());
    Mockito.verify(mockConsumer, Mockito.times(0)).seekToEnd(ArgumentMatchers.any());
    Mockito.verify(mockConsumer, Mockito.times(0))
        .seek(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job, 1500L);
  }

  @Test
  public void testSeekStartOffsetSeekToEnd() {
    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Job job =
        Job.newBuilder()
            .setJobId(0)
            .setFlowControl(
                FlowControl.newBuilder().setMessagesPerSec(1000).setBytesPerSec(1000).build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setAutoOffsetResetPolicy(AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST)
                    .setTopic(TOPIC)
                    .setPartition(0)
                    .setConsumerGroup(GROUP)
                    .build())
            .build();

    Map<TopicPartition, Job> topicPartitionJobMap = ImmutableMap.of(tp, job);
    Map<TopicPartition, Long> immutableSeekOffsetMap = ImmutableMap.of(tp, 1000L);

    // case 1: offset is between start offset and end offset
    Mockito.when(mockConsumer.beginningOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp, 1500L));
    Mockito.when(mockConsumer.endOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp, 2000L));
    Map<TopicPartition, Long> seekOffsetMap = new HashMap<>(immutableSeekOffsetMap);
    fetcherThread.seekStartOffset(seekOffsetMap, topicPartitionJobMap);
    Mockito.verify(mockConsumer, Mockito.times(0)).seekToBeginning(ArgumentMatchers.any());
    Mockito.verify(mockConsumer, Mockito.times(1)).seekToEnd(ArgumentMatchers.any());
    Mockito.verify(mockConsumer, Mockito.times(0))
        .seek(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job, 2000);
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
                .build())
        .setFlowControl(
            FlowControl.newBuilder()
                .setMessagesPerSec(messageRateQuota)
                .setBytesPerSec(byteRateQuota)
                .build())
        .build();
  }
}
