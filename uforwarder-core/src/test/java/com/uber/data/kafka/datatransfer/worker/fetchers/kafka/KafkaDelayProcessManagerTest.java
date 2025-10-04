package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class KafkaDelayProcessManagerTest<K, V> extends FievelTestBase {

  private Scope scope;
  private KafkaDelayProcessManager delayProcessManager;
  private KafkaConsumer mockConsumer;
  private String consumerGroup = "test-consumer-group";
  private TopicPartition topicPartition = new TopicPartition("test-topic", 1);
  private int currentTimeAdditionMs = 10;

  @BeforeEach
  public void setUp() {
    scope = Mockito.mock(Scope.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Counter counter = Mockito.mock(Counter.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    mockConsumer = Mockito.mock(KafkaConsumer.class);
  }

  @Test
  public void testShouldDelayProcessTrue() {
    int processingDelayMs = 10000; // 10 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    ConsumerRecord<byte[], byte[]> consumerRecord = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord.timestamp())
        .thenReturn(System.currentTimeMillis() + currentTimeAdditionMs);
    Assertions.assertTrue(delayProcessManager.shouldDelayProcess(consumerRecord));
  }

  @Test
  public void testShouldDelayProcessFalse() {
    int processingDelayMs = 10000; // 10 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    ConsumerRecord<byte[], byte[]> consumerRecord = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord.timestamp())
        .thenReturn(System.currentTimeMillis() - processingDelayMs - currentTimeAdditionMs);
    Assertions.assertFalse(delayProcessManager.shouldDelayProcess(consumerRecord));
  }

  @Test
  public void testShouldDelayProcessFalseWhenDelayMsNegative() {
    int processingDelayMs = -1; // negative value
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    ConsumerRecord<byte[], byte[]> consumerRecord = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord.timestamp())
        .thenReturn(System.currentTimeMillis() + currentTimeAdditionMs);
    Assertions.assertFalse(delayProcessManager.shouldDelayProcess(consumerRecord));
  }

  @Test
  public void testGetRecords() {
    int processingDelayMs = 10000; // 10 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    List<ConsumerRecord<K, V>> unprocessedRecords = new ArrayList<>();
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));

    delayProcessManager.pausedPartitionsAndRecords(topicPartition, unprocessedRecords);
    Assertions.assertEquals(2, delayProcessManager.getRecords(topicPartition).size());

    TopicPartition topicPartition2 = new TopicPartition("test-topic", 2);
    Assertions.assertEquals(0, delayProcessManager.getRecords(topicPartition2).size());
  }

  @Test
  public void testPause() {
    int processingDelayMs = 10000; // 10 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    List<ConsumerRecord<K, V>> unprocessedRecords = new ArrayList<>();
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));

    delayProcessManager.pausedPartitionsAndRecords(topicPartition, unprocessedRecords);

    List<TopicPartition> pausedPartitions = delayProcessManager.getAll();
    Assertions.assertEquals(1, pausedPartitions.size());
    Assertions.assertEquals(topicPartition, pausedPartitions.get(0));
    delayProcessManager
        .getRecords(topicPartition)
        .forEach(record -> Assertions.assertTrue(unprocessedRecords.contains(record)));
  }

  @Test
  public void testPauseWithRepeatTopiPartitions() {
    int processingDelayMs = 10000; // 10 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    delayProcessManager.pausedPartitionsAndRecords(topicPartition, new ArrayList<>());
    List<ConsumerRecord<K, V>> unprocessedRecords1 = new ArrayList<>();
    unprocessedRecords1.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords1.add(Mockito.mock(ConsumerRecord.class));
    delayProcessManager.pausedPartitionsAndRecords(topicPartition, unprocessedRecords1);

    List<ConsumerRecord<K, V>> unprocessedRecords2 = new ArrayList<>();
    unprocessedRecords2.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords2.add(Mockito.mock(ConsumerRecord.class));
    try {
      delayProcessManager.pausedPartitionsAndRecords(topicPartition, unprocessedRecords2);
      Assertions.fail("The topic partition test-topic-1 is already in delayedRecords");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testPauseWithEmtpyUnprocessedRecords() {
    int processingDelayMs = 10000; // 10 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    delayProcessManager.pausedPartitionsAndRecords(topicPartition, new ArrayList<>());
    Mockito.verify(mockConsumer, Mockito.times(0)).pause(Collections.singleton(topicPartition));
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  @Timeout(value = 50000, unit = TimeUnit.MILLISECONDS)
  public void testResume() throws InterruptedException {
    int processingDelayMs = 5000; // 5 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);

    List<ConsumerRecord<K, V>> unprocessedRecords1 = new ArrayList<>();
    ConsumerRecord<K, V> consumerRecord1 = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord1.timestamp()).thenReturn(System.currentTimeMillis());
    unprocessedRecords1.add(consumerRecord1);
    delayProcessManager.pausedPartitionsAndRecords(topicPartition, unprocessedRecords1);

    List<ConsumerRecord<K, V>> unprocessedRecords2 = new ArrayList<>();
    TopicPartition topicPartition2 = new TopicPartition("test-topic", 2);
    ConsumerRecord<K, V> consumerRecord2 = Mockito.mock(ConsumerRecord.class);
    Mockito.when(consumerRecord2.timestamp())
        .thenReturn(System.currentTimeMillis() + 25000); // 25 seconds
    unprocessedRecords2.add(consumerRecord2);
    delayProcessManager.pausedPartitionsAndRecords(topicPartition2, unprocessedRecords2);

    Thread.sleep(10000); // 10 seconds

    Map<TopicPartition, List<ConsumerRecord<K, V>>> resumedRecords1 =
        delayProcessManager.resumePausedPartitionsAndRecords();
    Assertions.assertEquals(1, resumedRecords1.size());
    Assertions.assertEquals(topicPartition, resumedRecords1.keySet().iterator().next());
    Assertions.assertEquals(unprocessedRecords1, resumedRecords1.values().iterator().next());

    // Processed topic partitions should be deleted in the next round
    delayProcessManager.resumePausedPartitionsAndRecords();
    Assertions.assertEquals(1, delayProcessManager.getAll().size());
    Assertions.assertEquals(topicPartition2, delayProcessManager.getAll().get(0));
  }

  @Test
  public void testDelete() {
    int processingDelayMs = 10000; // 10 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    List<ConsumerRecord<K, V>> unprocessedRecords = new ArrayList<>();
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));
    delayProcessManager.pausedPartitionsAndRecords(topicPartition, unprocessedRecords);

    TopicPartition topicPartition2 = new TopicPartition("test-topic", 2);
    List<ConsumerRecord<K, V>> unprocessedRecords2 = new ArrayList<>();
    unprocessedRecords2.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords2.add(Mockito.mock(ConsumerRecord.class));
    delayProcessManager.pausedPartitionsAndRecords(topicPartition2, unprocessedRecords2);

    delayProcessManager.delete(Collections.singletonList(topicPartition));
    List<TopicPartition> topicPartitions = delayProcessManager.getAll();
    Assertions.assertEquals(1, topicPartitions.size());
    Assertions.assertEquals(topicPartition2, topicPartitions.get(0));
  }

  @Test
  public void testGetAll() {
    int processingDelayMs = 10000; // 10 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    List<ConsumerRecord<K, V>> unprocessedRecords = new ArrayList<>();
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));
    delayProcessManager.pausedPartitionsAndRecords(topicPartition, unprocessedRecords);
    delayProcessManager.close();
    Assertions.assertTrue(delayProcessManager.getAll().isEmpty());
  }

  @Test
  public void testClose() {
    int processingDelayMs = 10000; // 10 seconds
    delayProcessManager =
        new KafkaDelayProcessManager<>(scope, consumerGroup, processingDelayMs, mockConsumer);
    List<ConsumerRecord<K, V>> unprocessedRecords = new ArrayList<>();
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));
    unprocessedRecords.add(Mockito.mock(ConsumerRecord.class));
    delayProcessManager.pausedPartitionsAndRecords(topicPartition, unprocessedRecords);
    delayProcessManager.close();
    Assertions.assertTrue(delayProcessManager.getAll().isEmpty());
  }
}
