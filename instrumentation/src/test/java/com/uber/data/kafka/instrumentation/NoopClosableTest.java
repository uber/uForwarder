package com.uber.data.kafka.instrumentation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NoopClosableTest {
  private NoopClosable noopClosable;

  @BeforeEach
  public void setup() {
    this.noopClosable = new NoopClosable();
  }

  @Test
  public void testClose() throws Exception {
    // this should not throw exception.
    noopClosable.close();
  }
}
