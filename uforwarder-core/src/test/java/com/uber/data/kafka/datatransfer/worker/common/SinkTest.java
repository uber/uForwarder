package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SinkTest extends FievelTestBase {
  private Sink sink;

  @Before
  public void setup() {
    sink = new Sink() {};
  }

  @Test
  public void testSubmit() {
    Assert.assertTrue(
        sink.submit(ItemAndJob.of("foo", Job.newBuilder().build()))
            .toCompletableFuture()
            .isCompletedExceptionally());
  }

  @Test
  public void testStart() {
    sink.start();
  }

  @Test
  public void testStop() {
    sink.stop();
  }

  @Test
  public void testIsRunning() {
    Assert.assertTrue(sink.isRunning());
  }
}
