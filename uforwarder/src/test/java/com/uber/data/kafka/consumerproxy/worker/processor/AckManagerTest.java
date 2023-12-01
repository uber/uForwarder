package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class AckManagerTest extends FievelTestBase {
  private Job job;
  private TopicPartitionOffset physicalKafkaMetadata;
  private ProcessorMessage processorMessage;
  private MessageAckStatusManager messageAckStatusManager;
  private UnprocessedMessageManager unprocessedMessageManager;
  private BlockingQueueStubManager stubManager;
  private AckManager ackManager;

  @Before
  public void setUp() {
    job =
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster("a")
                    .setConsumerGroup("b")
                    .setTopic("c")
                    .setPartition(1)
                    .build())
            .build();
    physicalKafkaMetadata = new TopicPartitionOffset("topic", 1, 2);
    processorMessage = Mockito.mock(ProcessorMessage.class);
    Mockito.when(processorMessage.getPhysicalMetadata()).thenReturn(physicalKafkaMetadata);
    Mockito.when(processorMessage.getValueByteSize()).thenReturn(10);
    messageAckStatusManager = Mockito.mock(MessageAckStatusManager.class);
    unprocessedMessageManager = Mockito.mock(UnprocessedMessageManager.class);
    stubManager = Mockito.mock(BlockingQueueStubManager.class);
    ackManager = new AckManager(messageAckStatusManager, unprocessedMessageManager, stubManager);
  }

  @Test
  public void testInit() {
    InOrder inOrder =
        Mockito.inOrder(stubManager, messageAckStatusManager, unprocessedMessageManager);
    ackManager.init(job);
    inOrder.verify(stubManager).init(job);
    inOrder.verify(messageAckStatusManager).init(job);
    inOrder.verify(unprocessedMessageManager).init(job);
  }

  @Test
  public void testCancel() {
    InOrder inOrder =
        Mockito.inOrder(messageAckStatusManager, unprocessedMessageManager, stubManager);
    ackManager.cancel(job);
    inOrder
        .verify(unprocessedMessageManager)
        .cancel(
            new TopicPartition(
                job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition()));
    inOrder.verify(messageAckStatusManager).cancel(job);
    inOrder
        .verify(stubManager)
        .cancel(
            new TopicPartition(
                job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition()));
  }

  @Test
  public void testCancelAll() {
    InOrder inOrder =
        Mockito.inOrder(messageAckStatusManager, unprocessedMessageManager, stubManager);
    ackManager.cancelAll();
    inOrder.verify(unprocessedMessageManager).cancelAll();
    inOrder.verify(messageAckStatusManager).cancelAll();
    inOrder.verify(stubManager).cancelAll();
  }

  @Test
  public void testReceive() {
    InOrder inOrder =
        Mockito.inOrder(stubManager, messageAckStatusManager, unprocessedMessageManager);
    ackManager.receive(processorMessage);
    inOrder.verify(unprocessedMessageManager).receive(processorMessage);
    inOrder.verify(stubManager).receive(processorMessage);
    inOrder.verify(messageAckStatusManager).receive(processorMessage);
  }

  @Test
  public void testAck() {
    Mockito.when(stubManager.ack(processorMessage)).thenReturn(true);
    InOrder inOrder =
        Mockito.inOrder(messageAckStatusManager, unprocessedMessageManager, stubManager);
    ackManager.ack(processorMessage);
    inOrder.verify(messageAckStatusManager).ack(physicalKafkaMetadata);
    inOrder.verify(unprocessedMessageManager).remove(processorMessage);
    inOrder.verify(stubManager).ack(processorMessage);
  }

  @Test
  public void testNack() {
    ackManager.nack(physicalKafkaMetadata);
    Mockito.verify(messageAckStatusManager, Mockito.times(1)).nack(physicalKafkaMetadata);
    Mockito.verify(stubManager, Mockito.times(1)).nack(physicalKafkaMetadata);
  }

  @Test
  public void testPublishMetrics() {
    ackManager.publishMetrics();
    Mockito.verify(unprocessedMessageManager, Mockito.times(1)).publishMetrics();
    Mockito.verify(messageAckStatusManager, Mockito.times(1)).publishMetrics();
  }

  @Test
  public void testGetStubs() {
    ackManager.getStubs();
    Mockito.verify(stubManager, Mockito.times(1)).getStubs();
  }
}
