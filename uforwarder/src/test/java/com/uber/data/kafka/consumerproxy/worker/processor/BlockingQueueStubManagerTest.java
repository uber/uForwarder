package com.uber.data.kafka.consumerproxy.worker.processor;

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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class BlockingQueueStubManagerTest extends ProcessorTestBase {
  private BlockingQueueStubManager blockingQueueStubManager;
  private Job job1, job2, job3;
  private Scope scope;
  private BlockingQueue mockBlockingQueue;
  private BiConsumer<BlockingQueue.BlockingReason, CancelResult> mockListener;
  private TopicPartitionOffset physicalKafkaMetadata1;
  private TopicPartitionOffset physicalKafkaMetadata2;
  private TopicPartitionOffset physicalKafkaMetadata3;
  private ProcessorMessage pm1, pm2, pm3;
  private TopicPartition tp1, tp2, tp3;

  @Before
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
    blockingQueueStubManager = new BlockingQueueStubManager(scope);
    blockingQueueStubManager.setCancelListener(mockListener);
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
    blockingQueueStubManager.addBlockingQueue(mockBlockingQueue);
  }

  @Test
  public void testCancelWithoutDLQ() {
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp1))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 1),
                    BlockingQueue.BlockingReason.BLOCKING)));
    blockingQueueStubManager.init(job1);
    blockingQueueStubManager.receive(pm1);
    blockingQueueStubManager.ack(pm1);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp1);
    Mockito.verify(mockBlockingQueue, Mockito.never()).markCanceled(physicalKafkaMetadata1);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.errorCode == CancelResult.ErrorCode.JOB_NOT_SUPPORTED));
    blockingQueueStubManager.cancel(tp1);
  }

  @Test
  public void testCancelBlockingMessageWithRetryQueue() {
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp3))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 3),
                    BlockingQueue.BlockingReason.BLOCKING)));
    blockingQueueStubManager.addTokens(50);
    blockingQueueStubManager.init(job3);
    blockingQueueStubManager.receive(pm3);
    blockingQueueStubManager.ack(pm3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).markCanceled(physicalKafkaMetadata3);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.responseCode == DispatcherResponse.Code.RETRY));
    Assert.assertEquals(1, blockingQueueStubManager.getTokens());
    blockingQueueStubManager.cancel(tp3);
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
    Assert.assertEquals(51, blockingQueueStubManager.getTokens());
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
    Assert.assertEquals(49, blockingQueueStubManager.getTokens());
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
    Assert.assertEquals(51, blockingQueueStubManager.getTokens());
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
    Assert.assertEquals(51, blockingQueueStubManager.getTokens());
  }

  @Test(expected = IllegalStateException.class)
  public void testUnassignedPartition() {
    blockingQueueStubManager.receive(pm1);
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
    blockingQueueStubManager.init(job3);
    blockingQueueStubManager.receive(pm3);
    blockingQueueStubManager.ack(pm3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.never()).markCanceled(invalidMetadata);
  }

  @Test
  public void testReturnTokenWhenCancelFail() {
    blockingQueueStubManager.init(job3);
    blockingQueueStubManager.receive(pm2);
    pm2.getStub().cancel(DispatcherResponse.Code.RETRY);
    Mockito.when(mockBlockingQueue.detectBlockingMessage(tp3))
        .thenReturn(
            Optional.of(
                new BlockingQueue.BlockingMessage(
                    new TopicPartitionOffset(tp1.topic(), tp1.partition(), 2),
                    BlockingQueue.BlockingReason.BLOCKING)));
    blockingQueueStubManager.addTokens(50);
    blockingQueueStubManager.receive(pm3);
    blockingQueueStubManager.ack(pm3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).markCanceled(physicalKafkaMetadata2);
    CancelResult expectedResult =
        blockingQueueStubManager.new TokenCancelResult(DispatcherResponse.Code.RETRY, 50);
    expectedResult.close(true);
    Mockito.verify(mockListener, Mockito.times(1))
        .accept(
            Mockito.eq(BlockingQueue.BlockingReason.BLOCKING),
            Mockito.argThat(r -> r.responseCode == DispatcherResponse.Code.RETRY));
    // cancel failed and token returned
    Assert.assertEquals(51, blockingQueueStubManager.getTokens());
  }

  @Test
  public void testNack() {
    blockingQueueStubManager.init(job3);
    blockingQueueStubManager.nack(physicalKafkaMetadata3);
    Mockito.verify(mockBlockingQueue, Mockito.times(1)).detectBlockingMessage(tp3);
  }
}
