package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class KafkaFetcherTest {
  private AbstractKafkaFetcherThread<String, String> kafkaFetcherThread;
  private KafkaFetcher<String, String> kafkaFetcher;
  private Job job;

  @BeforeEach
  public void testSetup() {
    kafkaFetcherThread = Mockito.mock(AbstractKafkaFetcherThread.class);
    kafkaFetcher = new KafkaFetcher(kafkaFetcherThread);
    job = Job.newBuilder().build();
  }

  @Test
  public void testStart() {
    kafkaFetcher.start();
    Mockito.verify(kafkaFetcherThread, Mockito.times(1)).start();
  }

  @Test
  public void testStop() {
    kafkaFetcher.stop();
    Mockito.verify(kafkaFetcherThread, Mockito.times(1)).close();
  }

  @Test
  public void testIsRunning() {
    Mockito.when(kafkaFetcherThread.isRunning()).thenReturn(true);
    Assertions.assertTrue(kafkaFetcher.isRunning());
    Mockito.verify(kafkaFetcherThread, Mockito.times(1)).isRunning();
  }

  @Test
  public void testSetNextStage() {
    Sink<ConsumerRecord<String, String>, Long> sink = Mockito.mock(Sink.class);
    kafkaFetcher.setNextStage(sink);
    Mockito.verify(kafkaFetcherThread, Mockito.times(1)).setNextStage(sink);
  }

  @Test
  public void testSetPipelineStateManager() {
    PipelineStateManager pipelineStateManager = Mockito.mock(PipelineStateManager.class);
    kafkaFetcher.setPipelineStateManager(pipelineStateManager);
    Mockito.verify(kafkaFetcherThread, Mockito.times(1))
        .setPipelineStateManager(pipelineStateManager);
  }

  @Test
  public void testSignal() {
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(kafkaFetcherThread).signal();
    Assertions.assertTrue(kafkaFetcher.signal().toCompletableFuture().isDone());
  }
}
