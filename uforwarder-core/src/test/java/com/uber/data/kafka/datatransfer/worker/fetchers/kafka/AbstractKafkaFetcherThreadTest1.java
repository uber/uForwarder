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
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

// this class is for testing those two abstract methods
public class AbstractKafkaFetcherThreadTest1 extends FievelTestBase {
  private final String THREAD_NAME = "thread-name";
  private final String TOPIC = "topic";
  private final String GROUP = "group";

  private KafkaFetcherConfiguration kafkaFetcherConfiguration;
  private PipelineStateManager pipelineStateManager;
  private CheckpointManager checkpointManager;
  private ThroughputTracker throughputTracker;
  private KafkaConsumer mockConsumer;
  private CoreInfra coreInfra;
  private Map<TopicPartition, Job> topicPartitionJobMap;
  private Map<TopicPartition, Long> immutableSeekOffsetMap;
  private TopicPartition tp;
  private Job job;
  private AbstractKafkaFetcherThread fetcherThread;

  @Before
  public void setUp() {
    kafkaFetcherConfiguration = new KafkaFetcherConfiguration();
    checkpointManager = Mockito.mock(CheckpointManager.class);
    throughputTracker = Mockito.mock(ThroughputTracker.class);
    tp = new TopicPartition(TOPIC, 0);
    job = createConsumerJob(0, TOPIC, 0, GROUP, 2000, 1000);
    topicPartitionJobMap = ImmutableMap.of(tp, job);
    mockConsumer = Mockito.mock(KafkaConsumer.class);
    immutableSeekOffsetMap = ImmutableMap.of(tp, 1000L);
    Mockito.when(mockConsumer.beginningOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp, 100L));
    Mockito.when(mockConsumer.endOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of(tp, 2000L));
    Scope scope = Mockito.mock(Scope.class);
    coreInfra = CoreInfra.NOOP;
    Counter counter = Mockito.mock(Counter.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Timer timer = Mockito.mock(Timer.class);
    Stopwatch stopwatch = Mockito.mock(Stopwatch.class);
    Mockito.when(scope.subScope(ArgumentMatchers.anyString())).thenReturn(scope);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(timer.start()).thenReturn(stopwatch);
    pipelineStateManager =
        new KafkaPipelineStateManager(
            Job.newBuilder()
                .setKafkaConsumerTask(
                    KafkaConsumerTask.newBuilder().setConsumerGroup(GROUP).setTopic(TOPIC).build())
                .build(),
            scope);
  }

  @Test
  public void testSeekStartOffsetWithEmptyTopicPartitionJobMap() {
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra);
    fetcherThread.setPipelineStateManager(pipelineStateManager);

    fetcherThread.seekStartOffset(immutableSeekOffsetMap, ImmutableMap.of());
    Mockito.verify(mockConsumer, Mockito.never()).seekToBeginning(ArgumentMatchers.anyCollection());
    Mockito.verify(mockConsumer, Mockito.never()).seekToEnd(ArgumentMatchers.anyCollection());
    Mockito.verify(mockConsumer, Mockito.never())
        .seek(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
    Mockito.verify(checkpointManager, Mockito.never())
        .setFetchOffset(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
  }

  @Test
  public void testSeekStartOffsetWithNegativeOffset() {
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra);
    fetcherThread.setPipelineStateManager(pipelineStateManager);

    fetcherThread.seekStartOffset(ImmutableMap.of(tp, -1000L), topicPartitionJobMap);
    Mockito.verify(mockConsumer, Mockito.never()).seekToBeginning(ArgumentMatchers.anyCollection());
    Mockito.verify(mockConsumer, Mockito.never()).seekToEnd(ArgumentMatchers.anyCollection());
    Mockito.verify(mockConsumer, Mockito.never())
        .seek(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
    Mockito.verify(checkpointManager, Mockito.never())
        .setFetchOffset(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
  }

  @Test
  public void testSeekStartOffsetDifferentOffsetLog() {
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra);
    fetcherThread.setPipelineStateManager(pipelineStateManager);

    // for the case that offset < start offset
    fetcherThread.seekStartOffset(ImmutableMap.of(tp, 10L), topicPartitionJobMap);
    // for the case that offset > end offset
    fetcherThread.seekStartOffset(ImmutableMap.of(tp, 3000L), topicPartitionJobMap);
    // for the case that start offset and end offset are null
    Mockito.when(mockConsumer.beginningOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of());
    Mockito.when(mockConsumer.endOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of());
    fetcherThread.seekStartOffset(ImmutableMap.of(tp, 3000L), topicPartitionJobMap);
  }

  @Test
  public void testSeekToSpecifiedOffset() {
    fetcherThread =
        new AbstractKafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra,
            true) {
          @Override
          public SeekStartOffsetOption getSeekStartOffsetOption(
              long specifiedOffset,
              @Nullable Long earliestOffset,
              @Nullable Long latestOffset,
              AutoOffsetResetPolicy autoOffsetResetPolicy) {
            return SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET;
          }

          @Override
          public boolean handleEndOffsetAndDelay(
              ConsumerRecord consumerRecord,
              Job job,
              CheckpointManager checkpointManager,
              PipelineStateManager pipelineStateManager) {
            return false;
          }
        };
    fetcherThread.setPipelineStateManager(pipelineStateManager);

    fetcherThread.seekStartOffset(immutableSeekOffsetMap, topicPartitionJobMap);

    Mockito.verify(mockConsumer, Mockito.times(1)).seek(tp, 1000);
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job, 1000);
  }

  @Test
  public void testSeekToEarliestOffset() {
    fetcherThread =
        new AbstractKafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra,
            true) {
          @Override
          public SeekStartOffsetOption getSeekStartOffsetOption(
              long specifiedOffset,
              @Nullable Long earliestOffset,
              @Nullable Long latestOffset,
              AutoOffsetResetPolicy autoOffsetResetPolicy) {
            return SeekStartOffsetOption.SEEK_TO_EARLIEST_OFFSET;
          }

          @Override
          public boolean handleEndOffsetAndDelay(
              ConsumerRecord consumerRecord,
              Job job,
              CheckpointManager checkpointManager,
              PipelineStateManager pipelineStateManager) {
            return false;
          }
        };
    fetcherThread.setPipelineStateManager(pipelineStateManager);

    // test non-empty start offset
    fetcherThread.seekStartOffset(immutableSeekOffsetMap, topicPartitionJobMap);

    Mockito.verify(mockConsumer, Mockito.times(1)).seekToBeginning(Collections.singleton(tp));
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job, 100);

    // test empty start offset
    Mockito.when(mockConsumer.beginningOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of());
    fetcherThread.seekStartOffset(immutableSeekOffsetMap, topicPartitionJobMap);

    Mockito.verify(mockConsumer, Mockito.times(2)).seekToBeginning(Collections.singleton(tp));
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job, 100);
  }

  @Test
  public void testSeekToLatestOffset() {
    fetcherThread =
        new AbstractKafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra,
            true) {
          @Override
          public SeekStartOffsetOption getSeekStartOffsetOption(
              long specifiedOffset,
              @Nullable Long earliestOffset,
              @Nullable Long latestOffset,
              AutoOffsetResetPolicy autoOffsetResetPolicy) {
            return SeekStartOffsetOption.SEEK_TO_LATEST_OFFSET;
          }

          @Override
          public boolean handleEndOffsetAndDelay(
              ConsumerRecord consumerRecord,
              Job job,
              CheckpointManager checkpointManager,
              PipelineStateManager pipelineStateManager) {
            return false;
          }
        };
    fetcherThread.setPipelineStateManager(pipelineStateManager);

    // test non-empty end offset
    fetcherThread.seekStartOffset(immutableSeekOffsetMap, topicPartitionJobMap);

    Mockito.verify(mockConsumer, Mockito.times(1)).seekToEnd(Collections.singleton(tp));
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job, 2000);

    // test empty end offset
    Mockito.when(mockConsumer.endOffsets(ArgumentMatchers.anyCollection()))
        .thenReturn(ImmutableMap.of());
    fetcherThread.seekStartOffset(immutableSeekOffsetMap, topicPartitionJobMap);

    Mockito.verify(mockConsumer, Mockito.times(2)).seekToEnd(Collections.singleton(tp));
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job, 2000);
  }

  @Test
  public void testDoNotSeek() {
    fetcherThread =
        new AbstractKafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra,
            true) {
          @Override
          public SeekStartOffsetOption getSeekStartOffsetOption(
              long specifiedOffset,
              @Nullable Long earliestOffset,
              @Nullable Long latestOffset,
              AutoOffsetResetPolicy autoOffsetResetPolicy) {
            return SeekStartOffsetOption.DO_NOT_SEEK;
          }

          @Override
          public boolean handleEndOffsetAndDelay(
              ConsumerRecord consumerRecord,
              Job job,
              CheckpointManager checkpointManager,
              PipelineStateManager pipelineStateManager) {
            return false;
          }
        };
    fetcherThread.setPipelineStateManager(pipelineStateManager);
    Mockito.when(mockConsumer.committed(Mockito.any(TopicPartition.class)))
        .thenReturn(new OffsetAndMetadata(12345L, ""));

    fetcherThread.seekStartOffset(immutableSeekOffsetMap, topicPartitionJobMap);

    Mockito.verify(mockConsumer, Mockito.never()).seekToBeginning(ArgumentMatchers.anyCollection());
    Mockito.verify(mockConsumer, Mockito.never()).seekToEnd(ArgumentMatchers.anyCollection());
    Mockito.verify(mockConsumer, Mockito.times(1)).committed(Mockito.any(TopicPartition.class));
    Mockito.verify(mockConsumer, Mockito.never())
        .seek(ArgumentMatchers.any(), ArgumentMatchers.anyLong());
    Mockito.verify(checkpointManager, Mockito.times(1))
        .setCommittedOffset(ArgumentMatchers.any(), ArgumentMatchers.eq(12345L));
    Mockito.verify(checkpointManager, Mockito.times(1))
        .setFetchOffset(ArgumentMatchers.any(), ArgumentMatchers.eq(12345L));
  }

  @Test
  public void testHandleEndOffsetAndDelayReturnTrue() throws InterruptedException {
    fetcherThread =
        new AbstractKafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra,
            true) {
          @Override
          public SeekStartOffsetOption getSeekStartOffsetOption(
              long specifiedOffset,
              @Nullable Long earliestOffset,
              @Nullable Long latestOffset,
              AutoOffsetResetPolicy autoOffsetResetPolicy) {
            return SeekStartOffsetOption.DO_NOT_SEEK;
          }

          @Override
          public boolean handleEndOffsetAndDelay(
              ConsumerRecord consumerRecord,
              Job job,
              CheckpointManager checkpointManager,
              PipelineStateManager pipelineStateManager) {
            return true;
          }
        };
    fetcherThread.setPipelineStateManager(pipelineStateManager);
    Sink processor = Mockito.mock(Sink.class);
    Mockito.when(processor.isRunning()).thenReturn(true);
    fetcherThread.setNextStage(processor);

    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Job job = createConsumerJob(1, TOPIC, 0, GROUP, 1000, 1000);

    fetcherThread.processFetchedData(prepareForHandleEndOffsetAndDelay(), ImmutableMap.of(tp, job));
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job, 101);
  }

  @Test
  public void testHandleEndOffsetAndDelayReturnFalse() throws InterruptedException {
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra);
    fetcherThread.setPipelineStateManager(pipelineStateManager);
    Sink processor = Mockito.mock(Sink.class);
    Mockito.when(processor.submit(ArgumentMatchers.any()))
        .thenReturn(CompletableFuture.completedFuture(101L));
    fetcherThread.setNextStage(processor);

    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Job job = createConsumerJob(1, TOPIC, 0, GROUP, 1000, 1000);

    fetcherThread.processFetchedData(prepareForHandleEndOffsetAndDelay(), ImmutableMap.of(tp, job));
    Mockito.verify(checkpointManager, Mockito.times(1)).setFetchOffset(job, 102);
  }

  private ConsumerRecords prepareForHandleEndOffsetAndDelay() {
    ConsumerRecords consumerRecords = Mockito.mock(ConsumerRecords.class);
    Mockito.when(consumerRecords.isEmpty()).thenReturn(false);

    TopicPartition tp = new TopicPartition(TOPIC, 0);
    Set<TopicPartition> partitions = new HashSet<>();
    partitions.add(tp);
    Mockito.when(consumerRecords.partitions()).thenReturn(partitions);

    List<ConsumerRecord> consumerRecordList = new ArrayList<>();
    Mockito.when(consumerRecords.records(tp)).thenReturn(consumerRecordList);

    // prepare consumer records
    ConsumerRecord consumerRecord = Mockito.mock(ConsumerRecord.class);
    consumerRecordList.add(consumerRecord);
    Mockito.when(consumerRecord.topic()).thenReturn(TOPIC);
    Mockito.when(consumerRecord.partition()).thenReturn(0);
    Mockito.when(consumerRecord.offset()).thenReturn(101L);
    Mockito.when(consumerRecord.serializedValueSize()).thenReturn(1);

    return consumerRecords;
  }

  private Job createConsumerJob(
      long jobID,
      String topicName,
      int partitionId,
      String consumerGroup,
      long messageRateQuota,
      long byteRateQuota) {
    KafkaConsumerTask task =
        KafkaConsumerTask.newBuilder()
            .setTopic(topicName)
            .setPartition(partitionId)
            .setConsumerGroup(consumerGroup)
            .build();
    return Job.newBuilder()
        .setFlowControl(
            FlowControl.newBuilder()
                .setMessagesPerSec(messageRateQuota)
                .setBytesPerSec(byteRateQuota)
                .setMaxInflightMessages(1)
                .build())
        .setJobId(jobID)
        .setKafkaConsumerTask(task)
        .build();
  }
}
