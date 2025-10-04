package com.uber.data.kafka.datatransfer.controller.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LocalSequencerTest {
  private IdProvider sequencer;

  @BeforeEach
  public void setup() throws Exception {
    sequencer = new LocalSequencer();
  }

  @Test
  public void testNextLong() throws Exception {
    Assertions.assertEquals(1L, sequencer.getId(null));
    Assertions.assertEquals(2L, sequencer.getId(null));
  }

  @Test
  public void testLifecycle() {
    sequencer.start();
    Assertions.assertTrue(sequencer.isRunning());
    sequencer.stop();
  }
}
