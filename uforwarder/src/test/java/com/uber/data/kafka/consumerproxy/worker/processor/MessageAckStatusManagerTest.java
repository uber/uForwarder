package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class MessageAckStatusManagerTest extends ProcessorTestBase {
  private static final String TOPIC = "topic";
  private static final int ACK_TRACKING_QUEUE_SIZE = 1;
  private Job job1;
  private Job job2;
  private Job job3;
  private TopicPartitionOffset physicalKafkaMetadata1;
  private TopicPartitionOffset physicalKafkaMetadata2;
  private TopicPartitionOffset physicalKafkaMetadata3;
  private MessageAckStatusManager messageAckStatusManager;
  private Scope scope;
  private ProcessorMessage pm1, pm2, pm3;

  @BeforeEach
  public void setUp() throws Exception {
    physicalKafkaMetadata1 = new TopicPartitionOffset(TOPIC, 1, 0);
    physicalKafkaMetadata2 = new TopicPartitionOffset(TOPIC, 1, 1);
    physicalKafkaMetadata3 = new TopicPartitionOffset(TOPIC, 3, 0);
    job1 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder().setTopic(TOPIC).setPartition(1).build())
            .build();
    job2 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setTopic(TOPIC)
                    .setPartition(2)
                    .setIsolationLevel(IsolationLevel.ISOLATION_LEVEL_READ_COMMITTED)
                    .build())
            .build();
    job3 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder().setTopic(TOPIC).setPartition(3).build())
            .build();
    scope = Mockito.mock(Scope.class);
    Counter counter = Mockito.mock(Counter.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Timer timer = Mockito.mock(Timer.class);
    Stopwatch stopwatch = Mockito.mock(Stopwatch.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(timer.start()).thenReturn(stopwatch);
    messageAckStatusManager = new MessageAckStatusManager(ACK_TRACKING_QUEUE_SIZE, scope);
    messageAckStatusManager.init(job1);
    messageAckStatusManager.init(job2);
    pm1 = newProcessMessage(physicalKafkaMetadata1);
    pm2 = newProcessMessage(physicalKafkaMetadata2);
    pm3 = newProcessMessage(physicalKafkaMetadata3);
  }

  @Test
  public void testCancel() {
    // cancel job3
    messageAckStatusManager.cancel(job3);

    // cancel job1
    IllegalStateException exception = null;
    messageAckStatusManager.cancel(job1);

    try {
      messageAckStatusManager.receive(pm1);
    } catch (IllegalStateException e) {
      exception = e;
    }
    Assertions.assertNotNull(exception);

    // cancel job1 again
    messageAckStatusManager.cancel(job1);

    // cancel job2
    messageAckStatusManager.cancel(job2);
    Assertions.assertEquals(
        AckTrackingQueue.CANNOT_ACK, messageAckStatusManager.ack(physicalKafkaMetadata2));
  }

  @Test
  public void testCancelAll() {
    IllegalStateException exception = null;
    messageAckStatusManager.cancelAll();
    Assertions.assertFalse(messageAckStatusManager.nack(physicalKafkaMetadata1));
  }

  @Test
  public void testReceive() {
    messageAckStatusManager.receive(pm1);

    IllegalStateException exception = null;
    try {
      messageAckStatusManager.receive(pm3);
    } catch (IllegalStateException e) {
      exception = e;
    }
    Assertions.assertNotNull(exception);
  }

  @Test
  public void testReceiveAndInterrupt() throws InterruptedException {
    messageAckStatusManager.receive(pm1);
    Thread thread = new Thread(() -> messageAckStatusManager.receive(pm2));
    thread.start();
    thread.interrupt();
    thread.join(100);
  }

  @Test
  public void testAck() {
    Assertions.assertEquals(
        AckTrackingQueue.CANNOT_ACK, messageAckStatusManager.ack(physicalKafkaMetadata1));
  }

  @Test
  public void testNack() {
    Assertions.assertFalse(messageAckStatusManager.nack(physicalKafkaMetadata1));
  }

  @Test
  public void testDetectBlockingMessage() throws Exception {
    messageAckStatusManager =
        new MessageAckStatusManager(
            100, HeadBlockingDetector.newBuilder().setMinAckPercent(0.95), scope);
    messageAckStatusManager.init(job1);
    TopicPartition tp =
        new TopicPartition(
            physicalKafkaMetadata1.getTopic(), physicalKafkaMetadata1.getPartition());
    Optional<BlockingQueue.BlockingMessage> blockingMessage =
        messageAckStatusManager.detectBlockingMessage(tp);
    Assertions.assertFalse(blockingMessage.isPresent());
    for (int i = 0; i < 100; ++i) {
      ProcessorMessage pm = newProcessMessage(new TopicPartitionOffset(TOPIC, 1, i));
      messageAckStatusManager.receive(pm);
    }
    for (int i = 99; i > 0; --i) {
      messageAckStatusManager.ack(new TopicPartitionOffset(TOPIC, 1, i));
      blockingMessage = messageAckStatusManager.detectBlockingMessage(tp);
      if (i > 4) {
        Assertions.assertFalse(blockingMessage.isPresent());
      } else {
        Assertions.assertTrue(blockingMessage.isPresent());
      }
    }
  }

  @Test
  public void testDetectBlockingMessageUnAssigned() {
    TopicPartition tp = new TopicPartition("random-topic", physicalKafkaMetadata1.getPartition());
    Optional<BlockingQueue.BlockingMessage> blockingMessage =
        messageAckStatusManager.detectBlockingMessage(tp);
    Assertions.assertFalse(blockingMessage.isPresent());
  }

  @Test
  public void testMarkCanceled() throws Exception {
    messageAckStatusManager = new MessageAckStatusManager(100, scope);
    messageAckStatusManager.init(job1);
    for (int i = 0; i < 100; ++i) {
      ProcessorMessage pm = newProcessMessage(new TopicPartitionOffset(TOPIC, 1, i));
      messageAckStatusManager.receive(pm);
    }
    for (int i = 99; i > 0; --i) {
      messageAckStatusManager.ack(new TopicPartitionOffset(TOPIC, 1, i));
    }
    TopicPartition tp =
        new TopicPartition(
            physicalKafkaMetadata1.getTopic(), physicalKafkaMetadata1.getPartition());
    Optional<BlockingQueue.BlockingMessage> blockingMessage =
        messageAckStatusManager.detectBlockingMessage(tp);
    Assertions.assertTrue(blockingMessage.isPresent());
    Assertions.assertTrue(
        messageAckStatusManager.markCanceled(blockingMessage.get().getMetaData()));
    blockingMessage = messageAckStatusManager.detectBlockingMessage(tp);
    Assertions.assertFalse(blockingMessage.isPresent());
  }

  @Test
  public void testMarkCanceledUnAssigned() {
    Assertions.assertFalse(messageAckStatusManager.markCanceled(pm3.getPhysicalMetadata()));
  }

  @Test
  public void testPublishMetrics() {
    messageAckStatusManager.publishMetrics();
    Mockito.verify(scope, Mockito.atLeastOnce()).gauge("tracking-queue.blocking.load-factor");
  }
}
