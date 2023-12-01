package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ZKSequencerTest extends FievelTestBase {
  private TestingServer zkServer;
  private CuratorFramework zkClient;
  private IdProvider sequencer;

  @Before
  public void setup() throws Exception {
    int port = 0;
    // try at most 5 times
    for (int i = 0; i < 5; i++) {
      try {
        port = TestUtils.getRandomPort();
        zkServer = new TestingServer(port, true);
        break;
      } catch (IllegalStateException e) {
        if (i == 4) {
          throw e;
        }
      }
    }
    zkServer.start();
    zkClient = CuratorFrameworkFactory.newClient("localhost:" + port, new RetryOneTime(0));
    sequencer = new ZKSequencer(zkClient, "/sequencer/item");
    sequencer.start();
  }

  @After
  public void teardown() throws Exception {
    sequencer.stop();
  }

  @Test
  public void testLifecycle() {
    sequencer.start();
    Assert.assertTrue(sequencer.isRunning());
    sequencer.stop();
  }

  @Test
  public void testNextLong() throws Exception {
    Assert.assertEquals(1L, sequencer.getId(null));
    Assert.assertEquals(2L, sequencer.getId(null));
  }
}
