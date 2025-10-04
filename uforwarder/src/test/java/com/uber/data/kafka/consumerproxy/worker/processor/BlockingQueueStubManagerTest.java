package com.uber.data.kafka.consumerproxy.worker.processor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.ResqConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import java.util.Optional;
import java.util.function.BiConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class BlockingQueueStubManagerTest extends ProcessorTestBase {
  private BlockingQueueStubManager blockingQueueStubManager1,
      blockingQueueStubManager2,
      blockingQueueStubManager3;
  private Job job1, job2, job3;
  private Scope scope;
  private BlockingQueue mockBlockingQueue;
  private BiConsumer<BlockingQueue.BlockingReason, CancelResult> mockListener;
  private TopicPartitionOffset physicalKafkaMetadata1;
  private TopicPartitionOffset physicalKafkaMetadata2;
  private TopicPartitionOffset physicalKafkaMetadata3;
  private ProcessorMessage pm1, pm2, pm3;
  private TopicPartition tp1, tp2, tp3;

  @BeforeEach
  public void setUp() throws Exception {
    physicalKafkaMetadata1 = new TopicPartitionOffset(TOPIC, 2, 1);
    physicalKafkaMetadata2 = new TopicPartitionOffset(TOPIC, 2, 2);
    physicalKafkaMetadata3 = new TopicPartitionOffset(TOPIC, 2, 3);
    pm1 = newProcessMessage(physicalKafkaMetadata1);
    pm2 = newProcessMessage(physicalKafkaMetadata2);
    pm3 = newProcessMessage(physicalKafkaMetadata3);
    tp1 =
        new TopicPartition(
            physicalKafkaMetadata1.getTopic(), physicalKafkaMetadata1.getPartition());
    tp2 =
        new TopicPartition(
            physicalKafkaMetadata2.getTopic(), physicalKafkaMetadata2.getPartition());
    tp3 =
        new TopicPartition(
            physicalKafkaMetadata3.getTopic(), physicalKafkaMetadata3.getPartition());
    scope = Mockito.mock(Scope.class);
    Counter counter = Mockito.mock(Counter.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    mockListener = Mockito.mock(BiConsumer.class);
    Job.Builder builder = Job.newBuilder();
    job1 =
        builder
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder().setTopic(TOPIC).setPartition(2).build())
            .build();

    builder
        .setKafkaConsumerTask(
            KafkaConsumerTask.newBuilder().setTopic(TOPIC).setPartition(2).build())
        .getRpcDispatcherTaskBuilder()
        .setDlqTopic("dlq");
    job2 = builder.build();

    RetryQueue retryQueue = RetryQueue.newBuilder().setRetryQueueTopic("topic1").build();
    builder
        .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().setTopic(TOPIC).setPartition(2))
        .getRetryConfigBuilder()
        .setRetryEnabled(true)
        .addRetryQueues(retryQueue);
    job3 = builder.build();

    mockBlockingQueue = Mockito.mock(BlockingQueue.class);
    blockingQueueStubManager1 = new BlockingQueueStubManager(job1, scope);
    blockingQueueStubManager1.setCancelListener(mockListener);
    blockingQueueStubManager1.addBlockingQueue(mockBlockingQueue);
    blockingQueueStubManager2 = new BlockingQueueStubManager(job2, scope);
    blockingQueueStubManager2.setCancelListener(mockListener);
    blockingQueueStubManager2.addBlockingQueue(mockBlockingQueue);
    blockingQueueStubManager3 = new BlockingQueueStubManager(job3, scope);
    blockingQueueStubManager3.setCancelListener(mockListener);
    blockingQueueStubManager3.addBlockingQueue(mockBlockingQueue);
  }

  @Test
  public void testCancelWithoutDLQ() {
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp1))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 1),
                    BlockingQueue.BlockingReason.BLOCKING)));
    blockingQueueStubManager1.init(job1);
    blockingQueueStubManager1.receive(pm1);
    blockingQueueStubManager1.ack(pm1);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp1);
    Mockito.verify(mockBlockingQueue, Mockito.never()).markCanceled(physicalKafkaMetadata1);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.errorCode == CancelResult.ErrorCode.JOB_NOT_SUPPORTED));
    blockingQueueStubManager1.cancel(tp1);
  }

  @Test
  public void testCancelBlockingMessageWithRetryQueue() {
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp3))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 3),
                    BlockingQueue.BlockingReason.BLOCKING)));
    blockingQueueStubManager3.addTokens(50);
    blockingQueueStubManager3.init(job3);
    blockingQueueStubManager3.receive(pm3);
    blockingQueueStubManager3.ack(pm3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).markCanceled(physicalKafkaMetadata3);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.responseCode == DispatcherResponse.Code.RETRY));
    Assertions.assertEquals(1, blockingQueueStubManager3.getTokens());
    blockingQueueStubManager3.cancel(tp3);
  }

  @Test
  public void testCancelBlockingMessageFromRetryQueueWithoutResilienceQueue() {
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp3))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 3),
                    BlockingQueue.BlockingReason.BLOCKING)));
    RetryQueue retryQueue = RetryQueue.newBuilder().setRetryQueueTopic(TOPIC).build();
    Job.Builder builder = Job.newBuilder();
    builder
        .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().setTopic(TOPIC).setPartition(2))
        .getRetryConfigBuilder()
        .setRetryEnabled(true)
        .addRetryQueues(retryQueue);
    job3 = builder.build();

    BlockingQueueStubManager blockingQueueStubManager = new BlockingQueueStubManager(job3, scope);
    blockingQueueStubManager.setCancelListener(mockListener);
    blockingQueueStubManager.addBlockingQueue(mockBlockingQueue);

    blockingQueueStubManager.addTokens(50);
    blockingQueueStubManager.init(job3);
    blockingQueueStubManager.receive(pm3);
    blockingQueueStubManager.ack(pm3);

    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.times(0)).markCanceled(physicalKafkaMetadata3);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.errorCode == CancelResult.ErrorCode.JOB_NOT_SUPPORTED));
    Assertions.assertEquals(51, blockingQueueStubManager.getTokens());
  }

  @Test
  public void testCancelBlockingMessageFromRetryQueueWithResilienceQueue() {
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp3))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 3),
                    BlockingQueue.BlockingReason.BLOCKING)));
    RetryQueue retryQueue = RetryQueue.newBuilder().setRetryQueueTopic(TOPIC).build();
    Job.Builder builder = Job.newBuilder();
    builder
        .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().setTopic(TOPIC).setPartition(2))
        .setResqConfig(
            ResqConfig.newBuilder().setResqEnabled(true).setResqTopic("topic__resq").build())
        .getRetryConfigBuilder()
        .setRetryEnabled(true)
        .addRetryQueues(retryQueue);
    job3 = builder.build();

    BlockingQueueStubManager blockingQueueStubManager = new BlockingQueueStubManager(job3, scope);
    blockingQueueStubManager.setCancelListener(mockListener);
    blockingQueueStubManager.addBlockingQueue(mockBlockingQueue);

    blockingQueueStubManager.addTokens(50);
    blockingQueueStubManager.init(job3);
    blockingQueueStubManager.receive(pm3);
    blockingQueueStubManager.ack(pm3);

    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).markCanceled(physicalKafkaMetadata3);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.responseCode == DispatcherResponse.Code.RESQ));
    Assertions.assertEquals(49, blockingQueueStubManager.getTokens());
  }

  @Test
  public void testCancelBlockingMessageFromResilienceQueue() {
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp3))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 3),
                    BlockingQueue.BlockingReason.BLOCKING)));
    Job.Builder builder = Job.newBuilder();
    builder
        .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().setTopic(TOPIC).setPartition(2))
        .setResqConfig(ResqConfig.newBuilder().setResqEnabled(true).setResqTopic(TOPIC).build());
    job3 = builder.build();

    BlockingQueueStubManager blockingQueueStubManager = new BlockingQueueStubManager(job3, scope);
    blockingQueueStubManager.setCancelListener(mockListener);
    blockingQueueStubManager.addBlockingQueue(mockBlockingQueue);

    blockingQueueStubManager.addTokens(50);
    blockingQueueStubManager.init(job3);
    blockingQueueStubManager.receive(pm3);
    blockingQueueStubManager.ack(pm3);

    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.times(0)).markCanceled(physicalKafkaMetadata3);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.errorCode == CancelResult.ErrorCode.JOB_NOT_SUPPORTED));
    Assertions.assertEquals(51, blockingQueueStubManager.getTokens());
  }

  @Test
  public void testCancelBlockingMessageFromDeadLetterQueue() {
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp3))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 3),
                    BlockingQueue.BlockingReason.BLOCKING)));
    Job.Builder builder = Job.newBuilder();
    builder
        .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().setTopic(TOPIC).setPartition(2))
        .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().setDlqTopic(TOPIC).build())
        .setResqConfig(
            ResqConfig.newBuilder().setResqEnabled(true).setResqTopic("topic__resq").build());
    job3 = builder.build();

    BlockingQueueStubManager blockingQueueStubManager = new BlockingQueueStubManager(job3, scope);
    blockingQueueStubManager.setCancelListener(mockListener);
    blockingQueueStubManager.addBlockingQueue(mockBlockingQueue);

    blockingQueueStubManager.addTokens(50);
    blockingQueueStubManager.init(job3);
    blockingQueueStubManager.receive(pm3);
    blockingQueueStubManager.ack(pm3);

    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.times(0)).markCanceled(physicalKafkaMetadata3);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.errorCode == CancelResult.ErrorCode.JOB_NOT_SUPPORTED));
    Assertions.assertEquals(51, blockingQueueStubManager.getTokens());
  }

  @Test
  public void testUnassignedPartition() {
    assertThrows(IllegalStateException.class, () -> blockingQueueStubManager1.receive(pm1));
  }

  @Test
  public void testInvalidOffset() {
    TopicPartitionOffset invalidMetadata = new TopicPartitionOffset(TOPIC, 2, -1);
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp3))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), -1),
                    BlockingQueue.BlockingReason.BLOCKING)));
    // blockingQueueStubManager.addTokens(50);
    blockingQueueStubManager3.init(job3);
    blockingQueueStubManager3.receive(pm3);
    blockingQueueStubManager3.ack(pm3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.never()).markCanceled(invalidMetadata);
  }

  @Test
  public void testReturnTokenWhenCancelFail() {
    blockingQueueStubManager3.init(job3);
    blockingQueueStubManager3.receive(pm2);
    pm2.getStub().cancel(DispatcherResponse.Code.RETRY);
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp3))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 2),
                    BlockingQueue.BlockingReason.BLOCKING)));
    blockingQueueStubManager3.addTokens(50);
    blockingQueueStubManager3.receive(pm3);
    blockingQueueStubManager3.ack(pm3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).markCanceled(physicalKafkaMetadata2);
    CancelResult expectedResult =
        blockingQueueStubManager3.new TokenCancelResult(DispatcherResponse.Code.RETRY, 50);
    expectedResult.close(true);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.responseCode == DispatcherResponse.Code.RETRY));
    // cancel failed and token returned
    Assertions.assertEquals(51, blockingQueueStubManager3.getTokens());
  }

  @Test
  public void testNack() {
    blockingQueueStubManager3.init(job3);
    blockingQueueStubManager3.nack(physicalKafkaMetadata3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
  }
}
