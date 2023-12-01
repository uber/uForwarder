package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Before;
import org.junit.Test;

public class ChainableTest extends FievelTestBase {
  private Chainable chainable;
  private Sink sink;

  @Before
  public void setup() {
    chainable = new Chainable() {};
    sink = new Sink() {};
  }

  @Test
  public void setNextStage() {
    chainable.setNextStage(sink);
  }
}
