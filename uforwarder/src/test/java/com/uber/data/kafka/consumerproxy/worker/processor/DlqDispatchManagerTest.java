package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DlqDispatchManagerTest extends ProcessorTestBase {
  private DlqDispatchManager dlqDispatchManager;
  private Scope mockScope;
  private Gauge mockGauge;
  private Counter mockCounter;

  @BeforeEach
  public void setUp() throws Exception {
    mockScope = Mockito.mock(Scope.class);
    mockGauge = Mockito.mock(Gauge.class);
    mockCounter = Mockito.mock(Counter.class);
    Mockito.when(mockScope.tagged(Mockito.anyMap())).thenReturn(mockScope);
    Mockito.when(mockScope.gauge(Mockito.anyString())).thenReturn(mockGauge);
    Mockito.when(mockScope.counter(Mockito.anyString())).thenReturn(mockCounter);
    dlqDispatchManager = new DlqDispatchManager(mockScope);

    dlqDispatchManager.init(job);
  }

  @Test
  public void testCredit() {
    TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
    dlqDispatchManager.credit(tp, 1);
    Assertions.assertEquals(2, dlqDispatchManager.getTokens(tp));
  }

  @Test
  public void testAcquire() {
    TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
    Assertions.assertTrue(dlqDispatchManager.tryAcquire(tp, 1));
    Assertions.assertEquals(0, dlqDispatchManager.getTokens(tp));
    Mockito.verify(mockCounter, Mockito.times(1)).inc(1);

    Assertions.assertFalse(dlqDispatchManager.tryAcquire(tp, 1));
    Mockito.verify(mockCounter, Mockito.times(2)).inc(1);
  }

  @Test
  public void testCancel() {
    TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
    dlqDispatchManager.cancel(job);
    Assertions.assertTrue(dlqDispatchManager.tryAcquire(tp, 100));
  }

  @Test
  public void testCancelAll() {
    TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
    dlqDispatchManager.cancelAll();
    Assertions.assertTrue(dlqDispatchManager.tryAcquire(tp, 100));
  }

  @Test
  public void testPublishMetrics() {
    dlqDispatchManager.publishMetrics();
    Mockito.verify(mockGauge, Mockito.times(1)).update(1);

    dlqDispatchManager.cancelAll();
    dlqDispatchManager.publishMetrics();
    Mockito.verify(mockGauge, Mockito.times(1)).update(1);
  }
}
