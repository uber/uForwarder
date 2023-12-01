package com.uber.data.kafka.datatransfer.worker.processors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Assert;
import org.junit.Test;

public class TranslatingPushPushProcessorTest extends FievelTestBase {
  @Test
  public void enqueue() {
    Sink<String, Void> mockSink = mock(Sink.class);
    TranslatingPushPushProcessor<Integer, String> processor =
        new TranslatingPushPushProcessor<Integer, String>(
            new NoopScope(), i -> Integer.toString(i));
    when(mockSink.submit(ItemAndJob.of("0", Job.newBuilder().build())))
        .thenReturn(CompletableFuture.completedFuture(null));
    processor.setNextStage(mockSink);
    CompletionStage<Void> enqueueFuture =
        processor.submit(ItemAndJob.of(0, Job.newBuilder().build()));
    Assert.assertTrue(enqueueFuture.toCompletableFuture().isDone());
  }
}
