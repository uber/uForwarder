package com.uber.data.kafka.datatransfer.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RunningLifecycleTest {
  private RunningLifecycle lifecycle;

  @BeforeEach
  public void setup() {
    lifecycle = new RunningLifecycle() {};
  }

  @Test
  public void testLifecycle() {
    lifecycle.start();
    Assertions.assertTrue(lifecycle.isRunning());
    lifecycle.stop();
  }
}
