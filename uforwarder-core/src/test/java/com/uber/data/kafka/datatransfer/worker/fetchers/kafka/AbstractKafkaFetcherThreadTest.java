package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.datatransfer.worker.pipelines.KafkaPipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineLoadTracker;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class AbstractKafkaFetcherThreadTest extends FievelTestBase {
  private static final String THREAD_NAME = "AbstractKafkaFetcherThreadTest";
  private AbstractKafkaFetcherThread fetcherThread;
  private KafkaFetcherConfiguration kafkaFetcherConfiguration;
  private PipelineStateManager pipelineStateManager;
  private CheckpointManager checkpointManager;
  private ThroughputTracker throughputTracker;
  private CheckpointInfo checkpointInfo;
  private KafkaConsumer mockConsumer;
  private Sink processor;
  private CoreInfra coreInfra;
  private PipelineLoadTracker pipelineLoadTracker;

  private final String TOPIC_NAME = "AbstractKafkaFetcherThreadTest";
  private final String CONSUMER_GROUP = "testgroup1";

  @Before
  public void setup() {
    checkpointInfo = Mockito.mock(CheckpointInfo.class);
    checkpointManager = Mockito.mock(CheckpointManager.class);
    throughputTracker = Mockito.mock(ThroughputTracker.class);
    pipelineLoadTracker = Mockito.mock(PipelineLoadTracker.class);
    Mockito.when(checkpointManager.addCheckpointInfo(any())).thenReturn(checkpointInfo);
    Mockito.when(checkpointManager.getCheckpointInfo(any())).thenReturn(checkpointInfo);
    Mockito.when(checkpointManager.getCheckpointInfo(any())).thenReturn(checkpointInfo);
    Mockito.when(checkpointInfo.getStartingOffset()).thenReturn(1L);
    Mockito.when(throughputTracker.getThroughput(any()))
        .thenReturn(new ThroughputTracker.Throughput(1, 2));
    Mockito.when(pipelineLoadTracker.getLoad())
        .thenReturn(
            new PipelineLoadTracker.PipelineLoad() {
              @Override
              public double getCoreCpuUsage() {
                return 1.0;
              }

              @Override
              public double getCpuUsage() {
                return 1.0;
              }
            });
    kafkaFetcherConfiguration = new KafkaFetcherConfiguration();
    kafkaFetcherConfiguration.setPollTimeoutMs(10);
    Scope scope = Mockito.mock(Scope.class);
    coreInfra = CoreInfra.NOOP;
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
    Mockito.when(scope.histogram(ArgumentMatchers.anyString(), any())).thenReturn(histogram);
    Mockito.when(timer.start()).thenReturn(stopwatch);

    mockConsumer = Mockito.mock(KafkaConsumer.class);
    processor = Mockito.mock(Sink.class);
    Mockito.when(processor.isRunning()).thenReturn(true);
    pipelineStateManager =
        new KafkaPipelineStateManager(
            Job.newBuilder()
                .setKafkaConsumerTask(
                    KafkaConsumerTask.newBuilder()
                        .setConsumerGroup(CONSUMER_GROUP)
                        .setTopic(TOPIC_NAME)
                        .build())
                .build(),
            pipelineLoadTracker,
            scope);
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra);
    fetcherThread.setPipelineStateManager(pipelineStateManager);
    fetcherThread.setNextStage(processor);
  }

  @Test
  public void testStartWithoutProcessor() {
    fetcherThread.setNextStage(null);
    try {
      fetcherThread.start();
      Assert.fail("fetcherThread.start() should failed because of missing component");
    } catch (NullPointerException e) {
      Assert.assertEquals("sink required", e.getMessage());
    }
  }

  @Test
  public void testStartWithoutConfigManager() {
    fetcherThread.setPipelineStateManager(null);
    try {
      fetcherThread.start();
      Assert.fail("fetcherThread.start() should failed because of missing component");
    } catch (NullPointerException e) {
      Assert.assertEquals("pipeline config manager required", e.getMessage());
    }
  }

  @Test(expected = NullPointerException.class)
  public void testStartWithoutCheckpointManager() {
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            null,
            throughputTracker,
            mockConsumer,
            coreInfra);
    fetcherThread.start();
  }

  @Test(expected = NullPointerException.class)
  public void testStartWithoutThroughputTracker() {
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            null,
            mockConsumer,
            coreInfra);
    fetcherThread.start();
  }

  @Test
  public void testProcessorNotRunning() {
    Mockito.when(processor.isRunning()).thenReturn(false);
    fetcherThread.doWork();
    Mockito.verify(mockConsumer, Mockito.times(1)).close();
  }

  @Test
  public void testStartCloseIsRunning() {
    Assert.assertTrue(fetcherThread.isRunning());

    fetcherThread.start();
    Assert.assertTrue(fetcherThread.isRunning());

    fetcherThread.close();
    Assert.assertFalse(fetcherThread.isRunning());
    fetcherThread.close();
    Assert.assertFalse(fetcherThread.isRunning());
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testReportPartitionOwnership() throws ExecutionException, InterruptedException {
    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    pipelineStateManager.run(job).toCompletableFuture().get();

    fetcherThread.start();
    // give it some time to report metric
    Thread.sleep(100);
  }

  @Test
  public void testCommitOffsets() {
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> mapCaptor =
        ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<OffsetCommitCallback> callbackCaptor =
        ArgumentCaptor.forClass(OffsetCommitCallback.class);
    checkpointManager = new KafkaCheckpointManager(coreInfra.scope());
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra);

    // try to commit an empty map
    fetcherThread.setPipelineStateManager(pipelineStateManager);
    fetcherThread.commitOffsets(ImmutableMap.of());
    Mockito.verify(mockConsumer, Mockito.never()).commitAsync(ArgumentMatchers.anyMap(), any());

    // try to commit an non-empty map
    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    checkpointManager.setOffsetToCommit(job, 10L);
    TopicPartition tp = new TopicPartition(TOPIC_NAME, 0);
    fetcherThread.commitOffsets(ImmutableMap.of(tp, job));
    Mockito.verify(mockConsumer, Mockito.times(1))
        .commitAsync(mapCaptor.capture(), callbackCaptor.capture());

    checkpointInfo = checkpointManager.getCheckpointInfo(job);
    // nothing wrong when there is an exception.
    callbackCaptor.getValue().onComplete(mapCaptor.getValue(), new Exception("test"));
    Assert.assertEquals(0L, checkpointInfo.getCommittedOffset());
    // if commitAsync succeeds, committed offset is updated.
    callbackCaptor.getValue().onComplete(mapCaptor.getValue(), null);
    Assert.assertEquals(10L, checkpointInfo.getCommittedOffset());

    // try to commit an non-empty map with commitOffset equals to committed offset(expected
    // behavior: skip commit offset)
    job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    checkpointManager.setCommittedOffset(job, 10L);
    checkpointManager.setOffsetToCommit(job, 10L);
    tp = new TopicPartition(TOPIC_NAME, 0);
    fetcherThread.commitOffsets(ImmutableMap.of(tp, job));
    Mockito.verify(mockConsumer, Mockito.times(1))
        .commitAsync(mapCaptor.capture(), callbackCaptor.capture());
    Mockito.verify(mockConsumer, Mockito.times(0)).commitSync(mapCaptor.capture());
  }

  @Test
  public void testSyncCommitOffsets() {
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> mapCaptor =
        ArgumentCaptor.forClass(Map.class);
    checkpointManager = new KafkaCheckpointManager(coreInfra.scope());
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            mockConsumer,
            coreInfra,
            false);

    // try to commit an empty map
    fetcherThread.setPipelineStateManager(pipelineStateManager);
    fetcherThread.commitOffsets(ImmutableMap.of());
    Mockito.verify(mockConsumer, Mockito.never()).commitAsync(ArgumentMatchers.anyMap(), any());
    Mockito.verify(mockConsumer, Mockito.never()).commitSync(mapCaptor.capture());

    // try to commit an non-empty map
    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    checkpointManager.setOffsetToCommit(job, 10L);
    TopicPartition tp = new TopicPartition(TOPIC_NAME, 0);
    fetcherThread.commitOffsets(ImmutableMap.of(tp, job));
    Mockito.verify(mockConsumer, Mockito.times(1)).commitSync(mapCaptor.capture());

    // if commitSync succeeds, committed offset is updated.
    checkpointInfo = checkpointManager.getCheckpointInfo(job);
    Assert.assertEquals(10L, checkpointInfo.getCommittedOffset());

    Mockito.doThrow(new RuntimeException()).when(mockConsumer).commitSync(mapCaptor.capture());
    checkpointManager.setOffsetToCommit(job, 20L);
    fetcherThread.commitOffsets(ImmutableMap.of(tp, job));
    Mockito.verify(mockConsumer, Mockito.times(1)).commitSync(mapCaptor.capture());
    // if commitSync failed, committed offset remain the same.
    checkpointInfo = checkpointManager.getCheckpointInfo(job);
    Assert.assertEquals(10L, checkpointInfo.getCommittedOffset());
  }

  @Test
  public void testKCommitOnIdleFetcher() throws Exception {
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> mapCaptor =
        ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<OffsetCommitCallback> callbackCaptor =
        ArgumentCaptor.forClass(OffsetCommitCallback.class);
    checkpointManager = new KafkaCheckpointManager(coreInfra.scope());
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            DelayProcessManager.NOOP,
            mockConsumer,
            coreInfra,
            true,
            true);
    // try to commit an empty map
    fetcherThread.setPipelineStateManager(pipelineStateManager);

    // try to commit a non-empty map with commitOffset equals to committed offset(expected
    // behavior: commitAsync)
    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    checkpointManager.setCommittedOffset(job, 10L);
    checkpointManager.setOffsetToCommit(job, 10L);
    TopicPartition tp = new TopicPartition(TOPIC_NAME, 0);
    fetcherThread.commitOffsets(ImmutableMap.of(tp, job));
    Mockito.verify(mockConsumer, Mockito.times(1))
        .commitAsync(mapCaptor.capture(), callbackCaptor.capture());
    Mockito.verify(mockConsumer, Mockito.times(0)).commitSync(mapCaptor.capture());
  }

  @Test
  public void testAddToCheckPointManager() {
    TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
    Job job1 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(CONSUMER_GROUP)
                    .setTopic(TOPIC_NAME)
                    .setPartition(0)
                    .setStartOffset(-1L)
                    .setEndOffset(-1L)
                    .build())
            .build();
    TopicPartition tp2 = new TopicPartition(TOPIC_NAME, 1);
    Job job2 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(CONSUMER_GROUP)
                    .setTopic(TOPIC_NAME)
                    .setPartition(1)
                    .setStartOffset(1L)
                    .setEndOffset(1L)
                    .build())
            .build();
    TopicPartition tp3 = new TopicPartition(TOPIC_NAME, 2);
    Job job3 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(CONSUMER_GROUP)
                    .setTopic(TOPIC_NAME)
                    .setPartition(0)
                    .setStartOffset(1L)
                    .setEndOffset(-1L)
                    .build())
            .build();
    TopicPartition tp4 = new TopicPartition(TOPIC_NAME, 3);
    Job job4 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(CONSUMER_GROUP)
                    .setTopic(TOPIC_NAME)
                    .setPartition(0)
                    .setStartOffset(-1L)
                    .setEndOffset(1L)
                    .build())
            .build();
    fetcherThread.addToCheckPointManager(
        ImmutableMap.of(tp1, job1, tp2, job2, tp3, job3, tp4, job4), ImmutableMap.of());
  }

  @Test
  public void testAddToCheckPointManagerWithoutStartOffset() {
    TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
    Job job1 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(CONSUMER_GROUP)
                    .setTopic(TOPIC_NAME)
                    .setPartition(0)
                    .setStartOffset(-1L)
                    .setEndOffset(-1L)
                    .build())
            .build();
    CheckpointInfo checkpointInfo =
        new CheckpointInfo(job1, KafkaUtils.MAX_INVALID_START_OFFSET, null);
    Mockito.when(checkpointManager.addCheckpointInfo(any())).thenReturn(checkpointInfo);
    fetcherThread.addToCheckPointManager(
        ImmutableMap.of(tp1, job1), ImmutableMap.of(tp1, new OffsetAndMetadata(10L)));
    Assert.assertEquals(10, checkpointInfo.getCommittedOffset());
    Assert.assertEquals(10, checkpointInfo.getOffsetToCommit());
  }

  @Test
  public void testAddToCheckPointManagerWithRetrieveCommitOffsetOnFetcherInitialization() {
    TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
    Job job1 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(CONSUMER_GROUP)
                    .setTopic(TOPIC_NAME)
                    .setPartition(0)
                    .setStartOffset(-1L)
                    .setEndOffset(-1L)
                    .build())
            .build();

    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            DelayProcessManager.NOOP,
            mockConsumer,
            coreInfra,
            true,
            true);

    CheckpointInfo checkpointInfo =
        new CheckpointInfo(job1, KafkaUtils.MAX_INVALID_START_OFFSET, null);
    Mockito.when(checkpointManager.addCheckpointInfo(any())).thenReturn(checkpointInfo);
    fetcherThread.addToCheckPointManager(ImmutableMap.of(tp1, job1), ImmutableMap.of());
    Assert.assertEquals(KafkaUtils.MAX_INVALID_START_OFFSET, checkpointInfo.getCommittedOffset());
    Assert.assertEquals(KafkaUtils.MAX_INVALID_START_OFFSET, checkpointInfo.getOffsetToCommit());

    fetcherThread.addToCheckPointManager(ImmutableMap.of(tp1, job1), null);
    Assert.assertEquals(KafkaUtils.MAX_INVALID_START_OFFSET, checkpointInfo.getCommittedOffset());
    Assert.assertEquals(KafkaUtils.MAX_INVALID_START_OFFSET, checkpointInfo.getOffsetToCommit());

    fetcherThread.addToCheckPointManager(
        ImmutableMap.of(tp1, job1), Collections.singletonMap(tp1, null));
    Assert.assertEquals(KafkaUtils.MAX_INVALID_START_OFFSET, checkpointInfo.getCommittedOffset());
    Assert.assertEquals(KafkaUtils.MAX_INVALID_START_OFFSET, checkpointInfo.getOffsetToCommit());

    fetcherThread.addToCheckPointManager(
        ImmutableMap.of(tp1, job1), ImmutableMap.of(tp1, new OffsetAndMetadata(10L)));
    Assert.assertEquals(10L, checkpointInfo.getCommittedOffset());
    Assert.assertEquals(10L, checkpointInfo.getOffsetToCommit());

    checkpointInfo = new CheckpointInfo(job1, 20L, null);
    Mockito.when(checkpointManager.addCheckpointInfo(any())).thenReturn(checkpointInfo);
    fetcherThread.addToCheckPointManager(
        ImmutableMap.of(tp1, job1), ImmutableMap.of(tp1, new OffsetAndMetadata(10L)));
    Assert.assertEquals(20L, checkpointInfo.getCommittedOffset());
    Assert.assertEquals(20L, checkpointInfo.getOffsetToCommit());
  }

  @Test
  public void testGetBrokerCommittedOffset() {
    TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
    Mockito.when(mockConsumer.committed(ImmutableSet.of(tp1)))
        .thenReturn(ImmutableMap.of(tp1, new OffsetAndMetadata(10L)));
    Map<TopicPartition, OffsetAndMetadata> partitionOffsetAndMetadataMap =
        fetcherThread.getBrokerCommittedOffset(ImmutableSet.of(tp1));
    Assert.assertEquals(10L, partitionOffsetAndMetadataMap.get(tp1).offset());
  }

  @Test
  public void testGetBrokerCommittedOffsetWithError() {
    TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
    Mockito.when(mockConsumer.committed(ImmutableSet.of(tp1)))
        .thenThrow(new IllegalArgumentException("test"));
    Map<TopicPartition, OffsetAndMetadata> partitionOffsetAndMetadataMap =
        fetcherThread.getBrokerCommittedOffset(ImmutableSet.of(tp1));
    Assert.assertEquals(0, partitionOffsetAndMetadataMap.size());
  }

  @Test
  public void testKCommitOnIdleFetcherWithInitialization() throws Exception {
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            DelayProcessManager.NOOP,
            mockConsumer,
            coreInfra,
            true,
            true);

    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    Job.Builder jobBuilder = Job.newBuilder(job);
    jobBuilder.getKafkaConsumerTaskBuilder().setStartOffset(KafkaUtils.MAX_INVALID_START_OFFSET);
    job = jobBuilder.build();

    CheckpointInfo checkpointInfo =
        new CheckpointInfo(job, KafkaUtils.MAX_INVALID_START_OFFSET, null);
    Mockito.when(checkpointManager.addCheckpointInfo(any())).thenReturn(checkpointInfo);
    Mockito.when(checkpointManager.getCheckpointInfo(any())).thenReturn(checkpointInfo);
    Mockito.when(checkpointManager.getCheckpointInfo(any())).thenReturn(checkpointInfo);
    TopicPartition tp = new TopicPartition(TOPIC_NAME, 0);
    Mockito.when(mockConsumer.committed(ImmutableSet.of(tp)))
        .thenReturn(ImmutableMap.of(tp, new OffsetAndMetadata(10L)));

    pipelineStateManager.run(job).toCompletableFuture().get();
    fetcherThread.setPipelineStateManager(pipelineStateManager);
    fetcherThread.setNextStage(processor);

    Set<TopicPartition> hashSet = new HashSet<>();
    hashSet.add(tp);
    fetcherThread.doWork();
    Mockito.verify(mockConsumer, Mockito.times(1)).committed(Mockito.anySet());
    Mockito.verify(mockConsumer, Mockito.times(1)).commitAsync(any(), any());

    // Verifies that committed is not inovked again
    fetcherThread.doWork();
    Mockito.verify(mockConsumer, Mockito.times(1)).committed(Mockito.anySet());
    Mockito.verify(mockConsumer, Mockito.times(1)).commitAsync(any(), any());
  }

  @Test
  public void testProcessFetchedData() throws InterruptedException, ExecutionException {
    ConsumerRecords consumerRecords = Mockito.mock(ConsumerRecords.class);
    Mockito.when(consumerRecords.isEmpty()).thenReturn(false);

    TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
    TopicPartition tp2 = new TopicPartition(TOPIC_NAME, 1);

    Set<TopicPartition> partitions = new HashSet<>();
    partitions.add(tp1);
    partitions.add(tp2);
    Mockito.when(consumerRecords.partitions()).thenReturn(partitions);

    Map<TopicPartition, Job> taskMap = new HashMap<>();
    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    pipelineStateManager.run(job).toCompletableFuture().get();
    taskMap.put(tp1, job);

    List<ConsumerRecord> consumerRecordList = new ArrayList<>();
    Mockito.when(consumerRecords.records(tp1)).thenReturn(consumerRecordList);

    // when the consumer records are empty
    fetcherThread.processFetchedData(consumerRecords, taskMap);
    Mockito.verify(mockConsumer, Mockito.times(1)).pause(Collections.singleton(tp2));

    // prepare consumer records
    ConsumerRecord consumerRecord = Mockito.mock(ConsumerRecord.class);
    consumerRecordList.add(consumerRecord);
    Mockito.when(consumerRecord.topic()).thenReturn(TOPIC_NAME);
    Mockito.when(consumerRecord.partition()).thenReturn(0);
    Mockito.when(consumerRecord.offset()).thenReturn(101L);
    Mockito.when(consumerRecord.serializedValueSize()).thenReturn(1);

    // case 0: tp1 has records but is not in the task map
    fetcherThread.processFetchedData(consumerRecords, ImmutableMap.of(tp2, job));
    Mockito.verify(mockConsumer, Mockito.times(1)).pause(Collections.singleton(tp1));
    Mockito.verify(mockConsumer, Mockito.times(1)).seek(tp1, 101L);

    // case 1: tp2 is not tracked, so it's paused
    CompletableFuture completableFuture = CompletableFuture.completedFuture(101L);
    Mockito.when(processor.submit(Mockito.isA(ItemAndJob.class))).thenReturn(completableFuture);
    fetcherThread.processFetchedData(consumerRecords, taskMap);
    // tp2 pause is accumulated
    Mockito.verify(mockConsumer, Mockito.times(2)).pause(Collections.singleton(tp2));

    // case 2: offset 101 is bounded for tp1 but we don't care
    Mockito.when(checkpointInfo.bounded(101L)).thenReturn(true);
    fetcherThread.processFetchedData(consumerRecords, taskMap);
    // tp2 pause is accumulated
    Mockito.verify(mockConsumer, Mockito.times(3)).pause(Collections.singleton(tp2));
    // tp1 is paused
    Mockito.verify(mockConsumer, Mockito.times(1)).pause(Collections.singleton(tp1));
    Mockito.verify(checkpointManager, Mockito.never()).setFetchOffset(job, 101);

    // message processing exception is handled
    completableFuture = new CompletableFuture();
    completableFuture.completeExceptionally(new Throwable());
    Mockito.when(processor.submit(Mockito.isA(ItemAndJob.class))).thenReturn(completableFuture);
    fetcherThread.processFetchedData(consumerRecords, taskMap);

    // case 3: job is updated, partition should not be paused
    Mockito.reset(mockConsumer);
    Job updatedJob = createConsumerJob(job.getJobId(), TOPIC_NAME, 0, CONSUMER_GROUP, 1002, 1002);
    pipelineStateManager.update(updatedJob).toCompletableFuture().get();
    fetcherThread.processFetchedData(consumerRecords, taskMap);
    Mockito.verify(mockConsumer, Mockito.never()).pause(Collections.singleton(tp1));
    Mockito.verify(mockConsumer, Mockito.times(1)).pause(Collections.singleton(tp2));
  }

  @Test
  public void testProcessFetchedDataWithCommitOffset()
      throws InterruptedException, ExecutionException {
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            DelayProcessManager.NOOP,
            mockConsumer,
            coreInfra,
            true,
            true);
    fetcherThread.setPipelineStateManager(pipelineStateManager);
    fetcherThread.setNextStage(processor);

    ConsumerRecords consumerRecords = Mockito.mock(ConsumerRecords.class);
    Mockito.when(consumerRecords.isEmpty()).thenReturn(false);

    TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
    TopicPartition tp2 = new TopicPartition(TOPIC_NAME, 1);

    Set<TopicPartition> partitions = new HashSet<>();
    partitions.add(tp1);
    partitions.add(tp2);
    Mockito.when(consumerRecords.partitions()).thenReturn(partitions);

    Map<TopicPartition, Job> taskMap = new HashMap<>();
    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    pipelineStateManager.run(job).toCompletableFuture().get();
    taskMap.put(tp1, job);

    List<ConsumerRecord> consumerRecordList = new ArrayList<>();
    Mockito.when(consumerRecords.records(tp1)).thenReturn(consumerRecordList);

    // prepare consumer records
    ConsumerRecord consumerRecord1 = Mockito.mock(ConsumerRecord.class);
    consumerRecordList.add(consumerRecord1);
    Mockito.when(consumerRecord1.topic()).thenReturn(TOPIC_NAME);
    Mockito.when(consumerRecord1.partition()).thenReturn(0);
    Mockito.when(consumerRecord1.offset()).thenReturn(101L);
    Mockito.when(consumerRecord1.serializedValueSize()).thenReturn(1);

    // prepare consumer records
    ConsumerRecord consumerRecord2 = Mockito.mock(ConsumerRecord.class);
    consumerRecordList.add(consumerRecord1);
    Mockito.when(consumerRecord2.topic()).thenReturn(TOPIC_NAME);
    Mockito.when(consumerRecord2.partition()).thenReturn(0);
    Mockito.when(consumerRecord2.offset()).thenReturn(102L);
    Mockito.when(consumerRecord2.serializedValueSize()).thenReturn(1);

    // mock checkpoint manager
    Mockito.when(checkpointManager.getOffsetToCommit(job)).thenReturn(100L).thenReturn(101L);
    // message processing exception is handled
    CompletableFuture completableFuture = CompletableFuture.completedFuture(101L);

    Mockito.when(processor.submit(Mockito.isA(ItemAndJob.class))).thenReturn(completableFuture);
    fetcherThread.processFetchedData(consumerRecords, taskMap);
    // only commit once since there is offsetCommitIntervalMs
    Mockito.verify(mockConsumer, Mockito.times(1)).commitAsync(any(), any());
  }

  @Test
  public void testLogTopicPartitionOffsetInfo() {
    kafkaFetcherConfiguration.setOffsetCommitIntervalMs(0);
    Assert.assertEquals(0, kafkaFetcherConfiguration.getOffsetCommitIntervalMs());

    // empty task map
    Map<TopicPartition, Job> taskMap = new HashMap<>();
    fetcherThread.logTopicPartitionOffsetInfo(taskMap);

    TopicPartition tp1 = new TopicPartition(TOPIC_NAME, 0);
    TopicPartition tp2 = new TopicPartition(TOPIC_NAME, 1);
    // no end offsets
    taskMap.put(tp1, createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000));
    taskMap.put(tp2, createConsumerJob(2, TOPIC_NAME, 1, CONSUMER_GROUP, 1000, 1000));
    Mockito.when(mockConsumer.assignment()).thenReturn(taskMap.keySet());
    fetcherThread.logTopicPartitionOffsetInfo(taskMap);
  }

  @Test
  public void testRefreshPartitionMap() throws ExecutionException, InterruptedException {
    Map<TopicPartition, Job> newTopicPartitionJobMap = new HashMap<>();
    Map<TopicPartition, Job> allTopicPartitionJobMap = new HashMap<>();
    Map<TopicPartition, Job> removedTopicPartitionJobMap = new HashMap<>();
    Job job1 = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    Job job2 = createConsumerJob(2, TOPIC_NAME, 1, CONSUMER_GROUP, 1000, 1000);
    Job job3 = createConsumerJob(3, TOPIC_NAME, 2, CONSUMER_GROUP, 1000, 1000);
    Job job4 = createConsumerJob(4, TOPIC_NAME, 2, CONSUMER_GROUP, 1, 1000);
    Job job5 = createConsumerJob(5, TOPIC_NAME, 2, CONSUMER_GROUP, 10, 1000);
    // job6 is the updated job
    Job job6 = createConsumerJob(5, TOPIC_NAME, 2, CONSUMER_GROUP, 20, 2000);

    // no add, no delete
    Assert.assertFalse(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(0, 0);
    Assert.assertEquals(0, newTopicPartitionJobMap.size());
    Assert.assertEquals(0, removedTopicPartitionJobMap.size());
    Assert.assertEquals(0, allTopicPartitionJobMap.size());

    // no add, no delete
    newTopicPartitionJobMap = new HashMap<>();
    allTopicPartitionJobMap = new HashMap<>();
    removedTopicPartitionJobMap = new HashMap<>();
    pipelineStateManager.run(job1).toCompletableFuture().get();
    validateMap(1, 0);
    Assert.assertTrue(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(1, 1);
    Assert.assertEquals(1, newTopicPartitionJobMap.size());
    Assert.assertEquals(1, allTopicPartitionJobMap.size());
    Assert.assertEquals(0, removedTopicPartitionJobMap.size());
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job1));

    // add 1 job
    newTopicPartitionJobMap = new HashMap<>();
    allTopicPartitionJobMap = new HashMap<>();
    removedTopicPartitionJobMap = new HashMap<>();
    pipelineStateManager.run(job2).toCompletableFuture().get();
    validateMap(2, 1);
    Assert.assertTrue(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(2, 2);
    Assert.assertEquals(1, newTopicPartitionJobMap.size());
    Assert.assertTrue(
        newTopicPartitionJobMap.containsKey(
            new TopicPartition(
                job2.getKafkaConsumerTask().getTopic(),
                job2.getKafkaConsumerTask().getPartition())));
    Assert.assertEquals(2, allTopicPartitionJobMap.size());
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job1));
    Assert.assertEquals(0, removedTopicPartitionJobMap.size());
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job2));

    // delete 1 non-existing job
    newTopicPartitionJobMap = new HashMap<>();
    allTopicPartitionJobMap = new HashMap<>();
    removedTopicPartitionJobMap = new HashMap<>();
    pipelineStateManager.cancel(job3).toCompletableFuture().get();
    validateMap(2, 2);
    Assert.assertFalse(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(2, 2);
    Assert.assertEquals(0, newTopicPartitionJobMap.size());
    Assert.assertEquals(2, allTopicPartitionJobMap.size());
    Assert.assertEquals(0, removedTopicPartitionJobMap.size());
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job1));
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job2));

    // delete 1 existing job
    newTopicPartitionJobMap = new HashMap<>();
    allTopicPartitionJobMap = new HashMap<>();
    removedTopicPartitionJobMap = new HashMap<>();
    pipelineStateManager.cancel(job2).toCompletableFuture().get();
    validateMap(1, 2);
    Assert.assertTrue(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(1, 1);
    Assert.assertEquals(0, newTopicPartitionJobMap.size());
    Assert.assertEquals(1, allTopicPartitionJobMap.size());
    Assert.assertEquals(1, removedTopicPartitionJobMap.size());
    Assert.assertTrue(removedTopicPartitionJobMap.containsValue(job2));
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job1));

    // add 1 job and cancel one job at the same time
    newTopicPartitionJobMap = new HashMap<>();
    allTopicPartitionJobMap = new HashMap<>();
    removedTopicPartitionJobMap = new HashMap<>();
    pipelineStateManager.run(job2).toCompletableFuture().get();
    pipelineStateManager.cancel(job1).toCompletableFuture().get();
    validateMap(1, 1);
    Assert.assertTrue(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(1, 1);
    Assert.assertEquals(1, newTopicPartitionJobMap.size());
    Assert.assertTrue(
        newTopicPartitionJobMap.containsKey(
            new TopicPartition(
                job2.getKafkaConsumerTask().getTopic(),
                job2.getKafkaConsumerTask().getPartition())));
    Assert.assertEquals(1, allTopicPartitionJobMap.size());
    Assert.assertEquals(1, removedTopicPartitionJobMap.size());
    Assert.assertTrue(removedTopicPartitionJobMap.containsValue(job1));
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job2));

    // add 2 jobs with the same topic-partition
    newTopicPartitionJobMap = new HashMap<>();
    allTopicPartitionJobMap = new HashMap<>();
    removedTopicPartitionJobMap = new HashMap<>();
    pipelineStateManager.run(job3).toCompletableFuture().get();
    pipelineStateManager.run(job4).toCompletableFuture().get();
    validateMap(3, 1);
    Assert.assertTrue(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(3, 3);
    Assert.assertEquals(1, newTopicPartitionJobMap.size());
    Assert.assertTrue(
        newTopicPartitionJobMap.containsKey(
            new TopicPartition(
                job3.getKafkaConsumerTask().getTopic(),
                job3.getKafkaConsumerTask().getPartition())));
    Assert.assertEquals(2, allTopicPartitionJobMap.size());
    Assert.assertEquals(0, removedTopicPartitionJobMap.size());
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job2));
    Assert.assertTrue(
        allTopicPartitionJobMap.containsValue(job3) || allTopicPartitionJobMap.containsValue(job4));

    newTopicPartitionJobMap = new HashMap<>();
    allTopicPartitionJobMap = new HashMap<>();
    removedTopicPartitionJobMap = new HashMap<>();
    pipelineStateManager.cancel(job1).toCompletableFuture().get();
    validateMap(3, 3);
    pipelineStateManager.update(job1).toCompletableFuture().get();
    validateMap(3, 3);
    pipelineStateManager.update(job3).toCompletableFuture().get();
    validateMap(3, 3);
    pipelineStateManager.update(job2).toCompletableFuture().get();
    validateMap(3, 3);
    Assert.assertFalse(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(3, 3);
    Assert.assertEquals(0, newTopicPartitionJobMap.size());
    Assert.assertEquals(2, allTopicPartitionJobMap.size());
    Assert.assertEquals(0, removedTopicPartitionJobMap.size());
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job2));

    // add 1 job with an existing topic-partition
    newTopicPartitionJobMap = new HashMap<>();
    allTopicPartitionJobMap = new HashMap<>();
    removedTopicPartitionJobMap = new HashMap<>();
    pipelineStateManager.run(job5).toCompletableFuture().get();
    validateMap(4, 3);
    Assert.assertTrue(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(4, 4);
    Assert.assertEquals(0, newTopicPartitionJobMap.size());
    Assert.assertEquals(2, allTopicPartitionJobMap.size());
    Assert.assertEquals(0, removedTopicPartitionJobMap.size());
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job2));
    Assert.assertTrue(
        allTopicPartitionJobMap.containsValue(job3)
            || allTopicPartitionJobMap.containsValue(job4)
            || allTopicPartitionJobMap.containsValue(job5));

    // update job5 with job6
    newTopicPartitionJobMap = new HashMap<>();
    allTopicPartitionJobMap = new HashMap<>();
    removedTopicPartitionJobMap = new HashMap<>();
    pipelineStateManager.run(job6).toCompletableFuture().get();
    validateMap(4, 4);
    // assignment is not changed
    Assert.assertFalse(
        fetcherThread.extractTopicPartitionMap(
            newTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap));
    validateMap(4, 4);
    Assert.assertEquals(0, newTopicPartitionJobMap.size());
    Assert.assertEquals(2, allTopicPartitionJobMap.size());
    Assert.assertEquals(0, removedTopicPartitionJobMap.size());
    Assert.assertTrue(allTopicPartitionJobMap.containsValue(job2));
    Assert.assertTrue(
        allTopicPartitionJobMap.containsValue(job3)
            || allTopicPartitionJobMap.containsValue(job4)
            || allTopicPartitionJobMap.containsValue(job6));
    Assert.assertFalse(allTopicPartitionJobMap.containsValue(job5));
  }

  @Test
  public void testDoWorkException() throws ExecutionException, InterruptedException {
    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    pipelineStateManager.run(job).toCompletableFuture().get();

    Mockito.doThrow(new RuntimeException()).when(mockConsumer).poll(any());
    fetcherThread.doWork();
  }

  @Test
  public void testPollException() throws ExecutionException, InterruptedException {
    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    pipelineStateManager.run(job).toCompletableFuture().get();

    Mockito.doThrow(new KafkaException()).when(mockConsumer).poll(any());
    fetcherThread.doWork();
  }

  @Test
  public void testKafkaConsumerFetcherThread() throws Exception {
    Assert.assertTrue(fetcherThread.isRunning());

    Job job = createConsumerJob(1, TOPIC_NAME, 0, CONSUMER_GROUP, 1000, 1000);
    Job.Builder jobBuilder = Job.newBuilder(job);
    jobBuilder.getKafkaConsumerTaskBuilder().setStartOffset(20L);
    job = jobBuilder.build();

    CheckpointInfo checkpointInfo = new CheckpointInfo(job, 20, null);
    Mockito.when(checkpointManager.addCheckpointInfo(any())).thenReturn(checkpointInfo);
    Mockito.when(checkpointManager.getCheckpointInfo(any())).thenReturn(checkpointInfo);
    Mockito.when(checkpointManager.getCheckpointInfo(any())).thenReturn(checkpointInfo);

    pipelineStateManager.run(job).toCompletableFuture().get();

    Set<TopicPartition> hashSet = new HashSet<>();
    hashSet.add(new TopicPartition(TOPIC_NAME, 0));
    // force the doWork()
    fetcherThread.doWork();
    Mockito.verify(mockConsumer, Mockito.atLeastOnce()).assign(hashSet);
    Mockito.verify(mockConsumer, Mockito.atLeastOnce()).poll(Duration.ofMillis(10L));

    List<JobStatus> jobStatusList = new ArrayList<>(pipelineStateManager.getJobStatus());
    Assert.assertEquals(1, jobStatusList.size());
    JobStatus jobStatus = jobStatusList.get(0);
    Assert.assertEquals(job, jobStatus.getJob());
    Assert.assertEquals(JobState.JOB_STATE_RUNNING, jobStatus.getState());
    Assert.assertEquals(1, jobStatus.getKafkaConsumerTaskStatus().getMessagesPerSec(), 0.0001);
    Assert.assertEquals(2, jobStatus.getKafkaConsumerTaskStatus().getBytesPerSec(), 0.0001);
    Assert.assertEquals(20L, jobStatus.getKafkaConsumerTaskStatus().getReadOffset());
    Assert.assertEquals(20L, jobStatus.getKafkaConsumerTaskStatus().getCommitOffset());
    Assert.assertEquals(1.0, jobStatus.getKafkaConsumerTaskStatus().getCpuUsage(), 0.0001);

    pipelineStateManager.cancelAll().toCompletableFuture().get();
    // force the doWork()
    fetcherThread.doWork();
    Mockito.verify(mockConsumer, Mockito.atLeastOnce()).assign(new HashSet<>());
  }

  @Test
  public void testReportKafkaConsumerMetrics() throws Exception {
    MetricName metricName =
        new MetricName("metric-name", "metric-group", "metric-description", ImmutableMap.of());
    Metric metric = Mockito.mock(Metric.class);
    Mockito.when(metric.metricName()).thenReturn(metricName);
    Mockito.when(metric.metricValue()).thenReturn(0.0);
    Mockito.when(mockConsumer.metrics()).thenReturn(ImmutableMap.of(metricName, metric));
    fetcherThread.reportKafkaConsumerMetrics();
  }

  @Test
  public void testReportKafkaConsumerMetricsNonDouble() throws Exception {
    MetricName metricName =
        new MetricName("metric-name", "metric-group", "metric-description", ImmutableMap.of());
    Metric metric = Mockito.mock(Metric.class);
    Mockito.when(metric.metricName()).thenReturn(metricName);
    Mockito.when(metric.metricValue()).thenReturn("not-double");
    Mockito.when(mockConsumer.metrics()).thenReturn(ImmutableMap.of(metricName, metric));
    fetcherThread.reportKafkaConsumerMetrics();
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

  private void validateMap(int jobRunningMapSize, int previousJobRunningMapSize) {
    Assert.assertEquals(jobRunningMapSize, pipelineStateManager.getExpectedRunningJobMap().size());
    Assert.assertEquals(previousJobRunningMapSize, fetcherThread.currentRunningJobMap.size());
  }
}
