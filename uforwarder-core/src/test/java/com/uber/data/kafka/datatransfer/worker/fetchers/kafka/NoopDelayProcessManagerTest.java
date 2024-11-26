package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class NoopDelayProcessManagerTest<K, V> extends FievelTestBase {
  private DelayProcessManager<K, V> delayProcessManager;

  @Before
  public void setUp() {
    delayProcessManager = new NoopDelayProcessManager<>();
  }

  @Test
  public void testShouldDelayProcess() {
    Assert.assertFalse(delayProcessManager.shouldDelayProcess(Mockito.any(ConsumerRecord.class)));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testPause() {
    delayProcessManager.pause(Mockito.any(TopicPartition.class), Mockito.anyList());
  }

  @Test
  public void testResume() {
    Assert.assertTrue(delayProcessManager.resume().isEmpty());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDelete() {
    delayProcessManager.delete(Mockito.anyCollection());
  }

  @Test
  public void testGetAll() {
    Assert.assertTrue(delayProcessManager.getAll().isEmpty());
  }
}
