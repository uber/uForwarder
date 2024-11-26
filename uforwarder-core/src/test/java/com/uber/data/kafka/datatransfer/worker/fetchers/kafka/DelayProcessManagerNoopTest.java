package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DelayProcessManagerNoopTest<K, V> extends FievelTestBase {
  private DelayProcessManager<K, V> delayProcessManager;

  @Before
  public void setUp() {
    delayProcessManager = DelayProcessManager.NOOP;
  }

  @Test
  public void testShouldDelayProcess() {
    Assert.assertFalse(delayProcessManager.shouldDelayProcess(Mockito.any(ConsumerRecord.class)));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testPause() {
    delayProcessManager.pausedPartitionsAndRecords(
        Mockito.any(TopicPartition.class), Mockito.anyList());
  }

  @Test
  public void testResume() {
    Assert.assertTrue(delayProcessManager.resumePausedPartitionsAndRecords().isEmpty());
  }

  @Test
  public void testDelete() {
    delayProcessManager.delete(Mockito.anyCollection());
  }

  @Test
  public void testGetAll() {
    Assert.assertTrue(delayProcessManager.getAll().isEmpty());
  }
}
