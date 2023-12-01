package com.uber.data.kafka.datatransfer.common;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RunningLifecycleTest extends FievelTestBase {
  private RunningLifecycle lifecycle;

  @Before
  public void setup() {
    lifecycle = new RunningLifecycle() {};
  }

  @Test
  public void testLifecycle() {
    lifecycle.start();
    Assert.assertTrue(lifecycle.isRunning());
    lifecycle.stop();
  }
}
