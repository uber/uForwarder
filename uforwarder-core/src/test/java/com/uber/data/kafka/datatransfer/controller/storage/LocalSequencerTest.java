package com.uber.data.kafka.datatransfer.controller.storage;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocalSequencerTest extends FievelTestBase {
  private IdProvider sequencer;

  @Before
  public void setup() throws Exception {
    sequencer = new LocalSequencer();
  }

  @Test
  public void testNextLong() throws Exception {
    Assert.assertEquals(1L, sequencer.getId(null));
    Assert.assertEquals(2L, sequencer.getId(null));
  }

  @Test
  public void testLifecycle() {
    sequencer.start();
    Assert.assertTrue(sequencer.isRunning());
    sequencer.stop();
  }
}
