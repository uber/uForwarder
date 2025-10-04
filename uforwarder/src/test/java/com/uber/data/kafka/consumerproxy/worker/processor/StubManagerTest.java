package com.uber.data.kafka.consumerproxy.worker.processor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StubManagerTest extends ProcessorTestBase {
  private StubManager stubManager;
  private Scope mockScope;
  private Counter mockInvalidPartitionCounter, mockInvalidOffsetCounter;

  @BeforeEach
  public void setUp() {
    mockScope = Mockito.mock(Scope.class);
    mockInvalidPartitionCounter = Mockito.mock(Counter.class);
    mockInvalidOffsetCounter = Mockito.mock(Counter.class);
    Mockito.when(mockScope.tagged(Mockito.anyMap())).thenReturn(mockScope);
    Mockito.when(mockScope.counter("processor.stub-message-manager.unassigned"))
        .thenReturn(mockInvalidPartitionCounter);
    Mockito.when(mockScope.counter("processor.stub-message-manager.invalid-offset"))
        .thenReturn(mockInvalidOffsetCounter);

    stubManager = new StubManager(mockScope);
  }

  @Test
  public void testCancel() {
    stubManager.init(job);
    stubManager.cancel(new TopicPartition(TOPIC, PARTITION));
  }

  @Test
  public void testCancelAll() {
    stubManager.init(job);
    stubManager.cancelAll();
  }

  @Test
  public void testCancelInvalidPartition() {
    stubManager.cancel(new TopicPartition(TOPIC, PARTITION));
    Mockito.verify(mockInvalidPartitionCounter).inc(1);
  }

  @Test
  public void testReceive() {
    stubManager.init(job);
    stubManager.receive(processorMessage);
  }

  @Test
  public void testReceiveInvalidPartition() {
    assertThrows(
        IllegalStateException.class,
        () -> {
          stubManager.receive(processorMessage);
          Mockito.verify(mockInvalidPartitionCounter).inc(1);
        });
  }

  @Test
  public void testRemove() {
    stubManager.init(job);
    stubManager.receive(processorMessage);
    stubManager.ack(processorMessage);
    Optional<MessageStub> stub = stubManager.getStub(processorMessage.getPhysicalMetadata());
    Assertions.assertFalse(stub.isPresent());
  }

  @Test
  public void testRemoveInvalidPartition() {
    stubManager.ack(processorMessage);
    Mockito.verify(mockInvalidPartitionCounter).inc(1);
  }

  @Test
  public void testRemoveInvalidMessage() {
    stubManager.init(job);
    stubManager.ack(processorMessage);
    Mockito.verify(mockInvalidOffsetCounter).inc(1);
  }

  @Test
  public void testGetStubs() {
    stubManager.init(job);
    stubManager.receive(processorMessage);
    Map<Job, Map<Long, MessageStub>> result = stubManager.getStubs();
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(1, result.get(job).size());
    stubManager.ack(processorMessage);
    result = stubManager.getStubs();
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(0, result.get(job).size());
  }
}
