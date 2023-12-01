package com.uber.data.kafka.instrumentation;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Before;
import org.junit.Test;

public class NoopClosableTest extends FievelTestBase {
  private NoopClosable noopClosable;

  @Before
  public void setup() {
    this.noopClosable = new NoopClosable();
  }

  @Test
  public void testClose() throws Exception {
    // this should not throw exception.
    noopClosable.close();
  }
}
