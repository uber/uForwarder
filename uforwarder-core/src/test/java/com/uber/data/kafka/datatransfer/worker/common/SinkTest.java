package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.data.kafka.datatransfer.Job;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SinkTest {
  private Sink sink;

  @BeforeEach
  public void setup() {
    sink = new Sink() {};
  }

  @Test
  public void testSubmit() {
    Assertions.assertTrue(
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
    Assertions.assertTrue(sink.isRunning());
  }
}
