package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import static org.mockito.ArgumentMatchers.any;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.CpuUsageMeter;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

// this test class uses for testing delay processing feature.
public class AbstractKafkaFetcherThreadTest3 extends FievelTestBase {

  private static final String THREAD_NAME = "AbstractKafkaFetcherThreadTest";
  private AbstractKafkaFetcherThread fetcherThread;
  private KafkaFetcherConfiguration kafkaFetcherConfiguration;
  private PipelineStateManager pipelineStateManager;
  private CheckpointManager checkpointManager;
  private ThroughputTracker throughputTracker;
  private DelayProcessManager delayProcessManager;
  private InflightMessageTracker inflightMessageTracker;
  private KafkaConsumer mockConsumer;
  private Sink processor;
  private CoreInfra coreInfra;

  private final String TOPIC_NAME = "AbstractKafkaFetcherThreadTest";
  private final String CONSUMER_GROUP = "testgroup1";
  private final int PROCESSING_DELAY_MS = 10000;

  @Before
  public void setup() {
    checkpointManager = new KafkaCheckpointManager(Mockito.mock(Scope.class));
    throughputTracker = Mockito.mock(ThroughputTracker.class);
    inflightMessageTracker = Mockito.mock(InflightMessageTracker.class);
    Mockito.when(throughputTracker.getThroughput(any()))
        .thenReturn(new ThroughputTracker.Throughput(1, 2));
    Mockito.when(inflightMessageTracker.getInflightMessageStats(any()))
        .thenReturn(new InflightMessageTracker.InflightMessageStats(1, 100));
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
    Mockito.when(processor.submit(ArgumentMatchers.any()))
        .thenReturn(CompletableFuture.completedFuture(10));
    pipelineStateManager =
        new KafkaPipelineStateManager(
            Job.newBuilder()
                .setKafkaConsumerTask(
                    KafkaConsumerTask.newBuilder()
                        .setConsumerGroup(CONSUMER_GROUP)
                        .setTopic(TOPIC_NAME)
                        .build())
                .build(),
            Mockito.mock(CpuUsageMeter.class),
            scope);
    delayProcessManager =
        new KafkaDelayProcessManager(scope, CONSUMER_GROUP, PROCESSING_DELAY_MS, mockConsumer);
    fetcherThread =
        new AbstractKafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            delayProcessManager,
            inflightMessageTracker,
            mockConsumer,
            coreInfra,
            true,
            false) {
          @Override
          public SeekStartOffsetOption getSeekStartOffsetOption(
              long specifiedOffset,
              @Nullable Long earliestOffset,
              @Nullable Long latestOffset,
              AutoOffsetResetPolicy autoOffsetResetPolicy) {
            return null;
          }

          @Override
          public boolean handleEndOffsetAndDelay(
              ConsumerRecord consumerRecord,
              Job job,
              CheckpointManager checkpointManager,
              PipelineStateManager pipelineStateManager)
              throws InterruptedException {
            return false;
          }
        };
    fetcherThread.setPipelineStateManager(pipelineStateManager);
    fetcherThread.setNextStage(processor);
  }

  @Test(expected = NullPointerException.class)
  public void testStartWithoutDelayProcessManage() {
    fetcherThread =
        new KafkaFetcherThread(
            THREAD_NAME,
            kafkaFetcherConfiguration,
            checkpointManager,
            throughputTracker,
            null,
            inflightMessageTracker,
            mockConsumer,
            coreInfra,
            true,
            true);
    fetcherThread.start();
  }

  @Test
  public void testProcessFetchedDataWithDelay() throws InterruptedException, ExecutionException {
    TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME, 1);
    List<ConsumerRecord> unprocessedRecords1 = new ArrayList<>();
    ConsumerRecord consumerRecord1 = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord1.topic()).thenReturn(TOPIC_NAME);
    Mockito.when(consumerRecord1.partition()).thenReturn(1);
    Mockito.when(consumerRecord1.offset()).thenReturn(1000001L);
    // reach delay time
    Mockito.when(consumerRecord1.timestamp())
        .thenReturn(System.currentTimeMillis() - PROCESSING_DELAY_MS);
    unprocessedRecords1.add(consumerRecord1);

    TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 2);
    List<ConsumerRecord> unprocessedRecords2 = new ArrayList<>();
    ConsumerRecord consumerRecord2 = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord2.topic()).thenReturn(TOPIC_NAME);
    Mockito.when(consumerRecord2.partition()).thenReturn(2);
    Mockito.when(consumerRecord2.offset()).thenReturn(2000001L);
    // has not yet reached delay time
    Mockito.when(consumerRecord2.timestamp())
        .thenReturn(System.currentTimeMillis() + 20000); // 20 seconds
    unprocessedRecords2.add(consumerRecord2);

    ConsumerRecords consumerRecords =
        new ConsumerRecords(
            ImmutableMap.of(
                topicPartition1, unprocessedRecords1, topicPartition2, unprocessedRecords2));

    Map<TopicPartition, Job> taskMap = new HashMap<>();
    Job job1 = createConsumerJob(1, TOPIC_NAME, 1, CONSUMER_GROUP, 1000, 1000);
    Job job2 = createConsumerJob(2, TOPIC_NAME, 2, CONSUMER_GROUP, 1000, 1000);
    pipelineStateManager.run(job1).toCompletableFuture().get();
    pipelineStateManager.run(job2).toCompletableFuture().get();
    taskMap.put(topicPartition1, job1);
    taskMap.put(topicPartition2, job2);

    fetcherThread.processFetchedData(consumerRecords, taskMap);

    Mockito.verify(processor, Mockito.times(1)).submit(any(ItemAndJob.class));
    Assert.assertEquals(1, delayProcessManager.getAll().size());
    Assert.assertTrue(delayProcessManager.getAll().contains(topicPartition2));
    List<ConsumerRecord> records =
        ((KafkaDelayProcessManager) delayProcessManager).getRecords(topicPartition2);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals(consumerRecord2, records.get(0));
    Assert.assertEquals(1000002, checkpointManager.getCheckpointInfo(job1).getFetchOffset());
    Assert.assertEquals(2000001, checkpointManager.getCheckpointInfo(job2).getFetchOffset());
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test(timeout = 60000)
  public void testProcessFetchedDataDelayComplete()
      throws InterruptedException, ExecutionException {
    TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME, 1);
    List<ConsumerRecord> unprocessedRecords1 = new ArrayList<>();
    ConsumerRecord consumerRecord1 = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord1.topic()).thenReturn(TOPIC_NAME);
    Mockito.when(consumerRecord1.partition()).thenReturn(1);
    Mockito.when(consumerRecord1.offset()).thenReturn(1000001L);
    // reach delay time
    Mockito.when(consumerRecord1.timestamp())
        .thenReturn(System.currentTimeMillis() - PROCESSING_DELAY_MS);
    unprocessedRecords1.add(consumerRecord1);

    TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 2);
    List<ConsumerRecord> unprocessedRecords2 = new ArrayList<>();
    ConsumerRecord consumerRecord2 = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord2.topic()).thenReturn(TOPIC_NAME);
    Mockito.when(consumerRecord2.partition()).thenReturn(2);
    Mockito.when(consumerRecord2.offset()).thenReturn(2000001L);
    // has not yet reached delay time
    Mockito.when(consumerRecord2.timestamp())
        .thenReturn(System.currentTimeMillis() + 20000); // 20 seconds
    unprocessedRecords2.add(consumerRecord2);

    ConsumerRecords consumerRecords =
        new ConsumerRecords(
            ImmutableMap.of(
                topicPartition1, unprocessedRecords1, topicPartition2, unprocessedRecords2));

    Map<TopicPartition, Job> taskMap = new HashMap<>();
    Job job1 = createConsumerJob(1, TOPIC_NAME, 1, CONSUMER_GROUP, 1000, 1000);
    Job job2 = createConsumerJob(2, TOPIC_NAME, 2, CONSUMER_GROUP, 1000, 1000);
    pipelineStateManager.run(job1).toCompletableFuture().get();
    pipelineStateManager.run(job2).toCompletableFuture().get();
    taskMap.put(topicPartition1, job1);
    taskMap.put(topicPartition2, job2);

    Thread.sleep(30000); // 30 seconds

    fetcherThread.processFetchedData(consumerRecords, taskMap);

    Mockito.verify(processor, Mockito.times(2)).submit(any(ItemAndJob.class));
    Assert.assertEquals(0, delayProcessManager.getAll().size());
    Assert.assertEquals(1000002, checkpointManager.getCheckpointInfo(job1).getFetchOffset());
    Assert.assertEquals(2000002, checkpointManager.getCheckpointInfo(job2).getFetchOffset());
  }

  @Test
  public void testMergeRecords() {
    TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME, 1);
    TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 2);
    TopicPartition topicPartition3 = new TopicPartition("test_topic", 1);
    TopicPartition topicPartition4 = new TopicPartition("test_topic", 2);
    List<ConsumerRecord> unprocessedRecords1 = new ArrayList<>();
    List<ConsumerRecord> unprocessedRecords2 = new ArrayList<>();
    List<ConsumerRecord> unprocessedRecords3 = new ArrayList<>();
    List<ConsumerRecord> unprocessedRecords4 = new ArrayList<>();
    unprocessedRecords1.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords1.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords2.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords3.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords4.add(Mockito.mock(ConsumerRecord.class));
    ConsumerRecords records =
        new ConsumerRecords(
            ImmutableMap.of(
                topicPartition1,
                unprocessedRecords1,
                topicPartition2,
                unprocessedRecords2,
                topicPartition3,
                unprocessedRecords3));

    Map<TopicPartition, List<ConsumerRecord>> resumedRecords =
        ImmutableMap.of(topicPartition4, unprocessedRecords4);
    ConsumerRecords consumerRecords = fetcherThread.mergeRecords(records, resumedRecords);

    Assert.assertEquals(5, consumerRecords.count());
    Set<TopicPartition> tps = consumerRecords.partitions();
    Assert.assertEquals(4, tps.size());
    Assert.assertTrue(tps.contains(topicPartition1));
    Assert.assertTrue(tps.contains(topicPartition2));
    Assert.assertTrue(tps.contains(topicPartition3));
    Assert.assertTrue(tps.contains(topicPartition4));
    Assert.assertEquals(2, consumerRecords.records(topicPartition1).size());
    Assert.assertEquals(1, consumerRecords.records(topicPartition2).size());
    Assert.assertEquals(1, consumerRecords.records(topicPartition3).size());
    Assert.assertEquals(1, consumerRecords.records(topicPartition4).size());
  }

  @Test
  public void testMergeRecordsRecordsNull() {
    TopicPartition topicPartition4 = new TopicPartition("test_topic", 2);
    List<ConsumerRecord> unprocessedRecords4 = new ArrayList<>();
    unprocessedRecords4.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords4.add(Mockito.mock(ConsumerRecord.class));

    Map<TopicPartition, List<ConsumerRecord>> resumedRecords =
        ImmutableMap.of(topicPartition4, unprocessedRecords4);
    ConsumerRecords consumerRecords = fetcherThread.mergeRecords(null, resumedRecords);

    Assert.assertEquals(2, consumerRecords.count());
    Set<TopicPartition> tps = consumerRecords.partitions();
    Assert.assertEquals(1, tps.size());
    Assert.assertTrue(tps.contains(topicPartition4));
    Assert.assertEquals(2, consumerRecords.records(topicPartition4).size());
  }

  @Test
  public void testMergeRecordsResumedRecordsEmpty() {
    TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME, 1);
    TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 2);
    TopicPartition topicPartition3 = new TopicPartition("test_topic", 1);
    List<ConsumerRecord> unprocessedRecords1 = new ArrayList<>();
    List<ConsumerRecord> unprocessedRecords2 = new ArrayList<>();
    List<ConsumerRecord> unprocessedRecords3 = new ArrayList<>();
    unprocessedRecords1.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords1.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords2.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords3.add(Mockito.mock(ConsumerRecord.class));
    ConsumerRecords records =
        new ConsumerRecords(
            ImmutableMap.of(
                topicPartition1,
                unprocessedRecords1,
                topicPartition2,
                unprocessedRecords2,
                topicPartition3,
                unprocessedRecords3));

    Map<TopicPartition, List<ConsumerRecord>> resumedRecords = Collections.emptyMap();
    ConsumerRecords consumerRecords = fetcherThread.mergeRecords(records, resumedRecords);

    Assert.assertEquals(4, consumerRecords.count());
    Set<TopicPartition> tps = consumerRecords.partitions();
    Assert.assertEquals(3, tps.size());
    Assert.assertTrue(tps.contains(topicPartition1));
    Assert.assertTrue(tps.contains(topicPartition2));
    Assert.assertTrue(tps.contains(topicPartition3));
    Assert.assertEquals(2, consumerRecords.records(topicPartition1).size());
    Assert.assertEquals(1, consumerRecords.records(topicPartition2).size());
    Assert.assertEquals(1, consumerRecords.records(topicPartition3).size());
  }

  @Test
  public void testMergeRecordsWithDuplication() {
    TopicPartition topicPartition1 = new TopicPartition(TOPIC_NAME, 1);
    TopicPartition topicPartition2 = new TopicPartition(TOPIC_NAME, 2);
    TopicPartition topicPartition3 = new TopicPartition("test_topic", 1);

    List<ConsumerRecord> unprocessedRecords1 = new ArrayList<>();
    List<ConsumerRecord> unprocessedRecords2 = new ArrayList<>();
    List<ConsumerRecord> unprocessedRecords3 = new ArrayList<>();
    List<ConsumerRecord> unprocessedRecords4 = new ArrayList<>();
    unprocessedRecords1.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords1.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords2.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords2.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords2.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords3.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords4.add(Mockito.mock(ConsumerRecord.class));

    ConsumerRecords records =
        new ConsumerRecords(
            ImmutableMap.of(
                topicPartition1,
                unprocessedRecords1,
                topicPartition2,
                unprocessedRecords2,
                topicPartition3,
                unprocessedRecords3));

    Map<TopicPartition, List<ConsumerRecord>> resumedRecords =
        ImmutableMap.of(topicPartition2, unprocessedRecords4); // duplication
    ConsumerRecords consumerRecords = fetcherThread.mergeRecords(records, resumedRecords);

    Assert.assertEquals(7, consumerRecords.count());
    Set<TopicPartition> tps = consumerRecords.partitions();
    Assert.assertEquals(3, tps.size());
    Assert.assertTrue(tps.contains(topicPartition1));
    Assert.assertTrue(tps.contains(topicPartition2));
    Assert.assertTrue(tps.contains(topicPartition3));

    Assert.assertEquals(2, consumerRecords.records(topicPartition1).size());
    Assert.assertEquals(4, consumerRecords.records(topicPartition2).size());
    Assert.assertEquals(1, consumerRecords.records(topicPartition3).size());
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
