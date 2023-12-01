package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ControllableTest extends FievelTestBase {
  private Job job;
  private Controllable controllable;

  @Before
  public void setup() throws Exception {
    job = Job.newBuilder().build();
    controllable = new Controllable() {};
  }

  @Test
  public void run() throws Exception {
    Assert.assertTrue(controllable.run(job).toCompletableFuture().isDone());
  }

  @Test
  public void cancel() {
    Assert.assertTrue(controllable.cancel(job).toCompletableFuture().isDone());
  }

  @Test
  public void update() {
    Assert.assertTrue(controllable.update(job).toCompletableFuture().isDone());
  }

  @Test
  public void cancelAll() {
    Assert.assertTrue(controllable.cancelAll().toCompletableFuture().isDone());
  }

  @Test
  public void getJobs() {
    Assert.assertTrue(controllable.getJobStatus().isEmpty());
  }

  @Test
  public void getJobStatus() {
    Assert.assertTrue(controllable.getJobs().isEmpty());
  }
}
