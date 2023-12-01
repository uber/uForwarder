package com.uber.data.kafka.consumerproxy.worker;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherImpl;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorImpl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcher;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PipelineImplTest extends FievelTestBase {
  private KafkaFetcher<byte[], byte[]> fetcher;
  private ProcessorImpl processor;
  private DispatcherImpl dispatcher;
  private PipelineStateManager stateManager;
  private PipelineImpl pipeline;
  private String pipelineId;

  @Before
  public void setup() {
    pipelineId = "mockPipelineId";
    fetcher = Mockito.mock(KafkaFetcher.class);
    processor = Mockito.mock(ProcessorImpl.class);
    dispatcher = Mockito.mock(DispatcherImpl.class);
    stateManager = Mockito.mock(PipelineStateManager.class);
    pipeline = new PipelineImpl(pipelineId, fetcher, processor, dispatcher, stateManager);

    Mockito.verify(fetcher, Mockito.times(1)).setNextStage(processor);
    Mockito.verify(processor, Mockito.times(1)).setNextStage(dispatcher);
    Mockito.verify(fetcher, Mockito.times(1)).setPipelineStateManager(stateManager);
    Mockito.verify(processor, Mockito.times(1)).setPipelineStateManager(stateManager);
  }

  @Test
  public void testStart() {
    pipeline.start();
    Mockito.verify(fetcher, Mockito.times(1)).start();
    Mockito.verify(processor, Mockito.times(1)).start();
    Mockito.verify(dispatcher, Mockito.times(1)).start();
  }

  @Test
  public void testStop() {
    pipeline.stop();
    Mockito.verify(fetcher, Mockito.times(1)).stop();
    Mockito.verify(processor, Mockito.times(1)).stop();
    Mockito.verify(dispatcher, Mockito.times(1)).stop();
    Mockito.verify(stateManager, Mockito.times(1)).clear();
  }

  @Test
  public void testIsRunning() {
    Mockito.doReturn(true).when(fetcher).isRunning();
    Mockito.doReturn(false).when(processor).isRunning();
    Mockito.doReturn(false).when(dispatcher).isRunning();
    Assert.assertFalse(pipeline.isRunning());

    Mockito.doReturn(true).when(processor).isRunning();
    Assert.assertFalse(pipeline.isRunning());

    Mockito.doReturn(true).when(dispatcher).isRunning();
    Assert.assertTrue(pipeline.isRunning());
  }

  @Test
  public void testRun() {
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(fetcher).signal();
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(stateManager).run(Mockito.any());
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(processor).run(Mockito.any());

    pipeline.run(Job.getDefaultInstance());

    Mockito.verify(processor, Mockito.times(2)).run(Job.getDefaultInstance());
    Mockito.verify(stateManager, Mockito.times(1)).run(Job.getDefaultInstance());
  }

  @Test(expected = ExecutionException.class)
  public void testRunWithException() throws Exception {
    CompletableFuture<Void> exceptionalCompletion = new CompletableFuture();
    exceptionalCompletion.completeExceptionally(new RuntimeException());
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(fetcher).signal();
    Mockito.doReturn(exceptionalCompletion).when(stateManager).run(Mockito.any());
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(processor).run(Mockito.any());

    pipeline.run(Job.getDefaultInstance()).toCompletableFuture().get();
  }

  @Test
  public void testUpdate() {
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(fetcher).signal();
    Mockito.doReturn(CompletableFuture.completedFuture(null))
        .when(stateManager)
        .update(Mockito.any());
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(processor).update(Mockito.any());

    pipeline.update(Job.getDefaultInstance());

    Mockito.verify(processor, Mockito.times(1)).update(Job.getDefaultInstance());
    Mockito.verify(stateManager, Mockito.times(1)).update(Job.getDefaultInstance());
  }

  @Test(expected = ExecutionException.class)
  public void testUpdateWithException() throws Exception {
    CompletableFuture<Void> exceptionalCompletion = new CompletableFuture();
    exceptionalCompletion.completeExceptionally(new RuntimeException());
    Mockito.doReturn(exceptionalCompletion).when(fetcher).signal();
    Mockito.doReturn(CompletableFuture.completedFuture(null))
        .when(stateManager)
        .update(Mockito.any());
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(processor).update(Mockito.any());

    pipeline.update(Job.getDefaultInstance()).toCompletableFuture().get();
  }

  @Test
  public void testCancel() {
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(fetcher).signal();
    Mockito.doReturn(CompletableFuture.completedFuture(null))
        .when(stateManager)
        .cancel(Mockito.any());
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(processor).cancel(Mockito.any());

    pipeline.cancel(Job.getDefaultInstance());

    Mockito.verify(processor, Mockito.times(1)).cancel(Job.getDefaultInstance());
    Mockito.verify(stateManager, Mockito.times(1)).cancel(Job.getDefaultInstance());
  }

  @Test(expected = ExecutionException.class)
  public void testCancelWithException() throws Exception {
    CompletableFuture<Void> exceptionalCompletion = new CompletableFuture();
    exceptionalCompletion.completeExceptionally(new RuntimeException());
    Mockito.doReturn(exceptionalCompletion).when(fetcher).signal();
    Mockito.doReturn(CompletableFuture.completedFuture(null))
        .when(stateManager)
        .cancel(Mockito.any());
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(processor).cancel(Mockito.any());

    pipeline.cancel(Job.getDefaultInstance()).toCompletableFuture().get();
  }

  @Test
  public void testCancelAll() {
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(fetcher).signal();
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(stateManager).cancelAll();
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(processor).cancelAll();

    pipeline.cancelAll();

    Mockito.verify(processor, Mockito.times(1)).cancelAll();
    Mockito.verify(stateManager, Mockito.times(1)).cancelAll();
  }

  @Test(expected = ExecutionException.class)
  public void testCancelAllWithException() throws Exception {
    CompletableFuture<Void> exceptionalCompletion = new CompletableFuture();
    exceptionalCompletion.completeExceptionally(new RuntimeException());
    Mockito.doReturn(exceptionalCompletion).when(fetcher).signal();
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(stateManager).cancelAll();
    Mockito.doReturn(CompletableFuture.completedFuture(null)).when(processor).cancelAll();

    pipeline.cancelAll().toCompletableFuture().get();
  }

  @Test
  public void testGetJobStatus() {
    List<JobStatus> expected = ImmutableList.of(JobStatus.getDefaultInstance());
    Mockito.doReturn(expected).when(stateManager).getJobStatus();
    Assert.assertEquals(expected, pipeline.getJobStatus());
  }

  @Test
  public void testGetJobs() {
    List<Job> expected = ImmutableList.of(Job.getDefaultInstance());
    Mockito.doReturn(expected).when(stateManager).getJobs();
    Assert.assertEquals(expected, pipeline.getJobs());
  }

  @Test
  public void testPublishMetrics() {
    pipeline.publishMetrics();
    Mockito.verify(processor, Mockito.times(1)).publishMetrics();
    Mockito.verify(stateManager, Mockito.times(1)).publishMetrics();
  }

  @Test
  public void getProcessor() {
    ProcessorImpl result = pipeline.processor();
    Assert.assertEquals(processor, result);
  }
}
