package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.data.kafka.datatransfer.Job;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ControllableTest {
  private Job job;
  private Controllable controllable;

  @BeforeEach
  public void setup() throws Exception {
    job = Job.newBuilder().build();
    controllable = new Controllable() {};
  }

  @Test
  public void run() throws Exception {
    Assertions.assertTrue(controllable.run(job).toCompletableFuture().isDone());
  }

  @Test
  public void cancel() {
    Assertions.assertTrue(controllable.cancel(job).toCompletableFuture().isDone());
  }

  @Test
  public void update() {
    Assertions.assertTrue(controllable.update(job).toCompletableFuture().isDone());
  }

  @Test
  public void cancelAll() {
    Assertions.assertTrue(controllable.cancelAll().toCompletableFuture().isDone());
  }

  @Test
  public void getJobs() {
    Assertions.assertTrue(controllable.getJobStatus().isEmpty());
  }

  @Test
  public void getJobStatus() {
    Assertions.assertTrue(controllable.getJobs().isEmpty());
  }
}
