package com.uber.data.kafka.datatransfer.worker.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ChainableTest {
  private Chainable chainable;
  private Sink sink;

  @BeforeEach
  public void setup() {
    chainable = new Chainable() {};
    sink = new Sink() {};
  }

  @Test
  public void setNextStage() {
    chainable.setNextStage(sink);
  }
}
