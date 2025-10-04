package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZKSequencerTest {
  private TestingServer zkServer;
  private CuratorFramework zkClient;
  private IdProvider sequencer;

  @BeforeEach
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

  @AfterEach
  public void teardown() throws Exception {
    sequencer.stop();
  }

  @Test
  public void testLifecycle() {
    sequencer.start();
    Assertions.assertTrue(sequencer.isRunning());
    sequencer.stop();
  }

  @Test
  public void testNextLong() throws Exception {
    Assertions.assertEquals(1L, sequencer.getId(null));
    Assertions.assertEquals(2L, sequencer.getId(null));
  }
}
