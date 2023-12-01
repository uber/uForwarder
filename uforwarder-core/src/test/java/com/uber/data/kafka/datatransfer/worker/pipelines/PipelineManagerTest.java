package com.uber.data.kafka.datatransfer.worker.pipelines;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class PipelineManagerTest extends FievelTestBase {
  private static final String PIPELINE_ID = "pipelineId";

  private PipelineManager pipelineManager;
  private PipelineFactory pipelineFactory;
  private Pipeline pipeline;
  private Pipeline otherPipeline;
  private Job job;
  private Job otherJob;
  private Scope scope;
  private Gauge gauge;

  @Before
  public void setup() {
    pipelineFactory = mock(PipelineFactory.class);
    job = Job.newBuilder().build();
    pipeline = mock(Pipeline.class);
    scope = mock(Scope.class);
    gauge = mock(Gauge.class);
    Counter counter = mock(Counter.class);
    Timer timer = mock(Timer.class);
    Stopwatch stopwatch = mock(Stopwatch.class);
    when(scope.subScope(ArgumentMatchers.anyString())).thenReturn(scope);
    when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    when(timer.start()).thenReturn(stopwatch);
    pipelineManager = new PipelineManager(scope, pipelineFactory);
    when(pipelineFactory.getPipelineId(job)).thenReturn(PIPELINE_ID);
    when(pipelineFactory.createPipeline(PIPELINE_ID, job)).thenReturn(pipeline);
    when(pipeline.isRunning()).thenReturn(true);
    JobStatus jobStatus = JobStatus.newBuilder().build();
    Mockito.when(pipeline.getJobStatus()).thenReturn(Arrays.asList(jobStatus));
  }

  @Test
  public void testGetOrCreatePipeline() {
    Assert.assertTrue(pipeline.isRunning());
    // the first time we create the pipeline, it will be created.
    pipelineManager.getOrCreatePipeline(PIPELINE_ID, job);
    Mockito.verify(pipelineFactory, Mockito.times(1)).createPipeline(PIPELINE_ID, job);
    // the second time we create the pipeline, the existing one will be used.
    pipelineManager.getOrCreatePipeline(PIPELINE_ID, job);
    Mockito.verify(pipelineFactory, Mockito.times(1)).createPipeline(PIPELINE_ID, job);
  }

  @Test
  public void testRun() {
    CompletableFuture mockFuture = CompletableFuture.completedFuture(null);
    when(pipeline.run(job)).thenReturn(mockFuture);
    CompletionStage<Void> completedFuture = pipelineManager.run(job);
    Assert.assertTrue(completedFuture.toCompletableFuture().isDone());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCancelled());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCompletedExceptionally());
    Assert.assertEquals(1, pipelineManager.getPipelines().size());

    // Rerun should be idempotent.
    completedFuture = pipelineManager.run(job);
    Assert.assertTrue(completedFuture.toCompletableFuture().isDone());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCancelled());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCompletedExceptionally());
    Assert.assertEquals(1, pipelineManager.getPipelines().size());

    // Run that completes exceptionally should be propagated back.
    mockFuture = new CompletableFuture();
    mockFuture.completeExceptionally(new Throwable());
    when(pipeline.run(job)).thenReturn(mockFuture);
    completedFuture = pipelineManager.run(job);
    Assert.assertTrue(completedFuture.toCompletableFuture().isDone());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCancelled());
    Assert.assertTrue(completedFuture.toCompletableFuture().isCompletedExceptionally());
    Assert.assertEquals(1, pipelineManager.getPipelines().size());
  }

  @Test(expected = RuntimeException.class)
  public void testRunWithException() {
    Mockito.doThrow(new RuntimeException())
        .when(pipelineFactory)
        .createPipeline(Mockito.anyString(), Mockito.any());
    pipelineManager.run(job);
  }

  @Test
  public void testCancel() {
    CompletionStage<Void> completedFuture = pipelineManager.cancel(job);
    Assert.assertTrue(completedFuture.toCompletableFuture().isDone());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCancelled());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCompletedExceptionally());
    Assert.assertEquals(0, pipelineManager.getPipelines().size());

    CompletableFuture mockFuture = CompletableFuture.completedFuture(null);
    when(pipeline.cancel(job)).thenReturn(mockFuture);
    pipelineManager.getOrCreatePipeline(pipelineFactory.getPipelineId(job), job);
    completedFuture = pipelineManager.cancel(job);
    Assert.assertTrue(completedFuture.toCompletableFuture().isDone());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCancelled());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCompletedExceptionally());
    Assert.assertEquals(1, pipelineManager.getPipelines().size());

    mockFuture = new CompletableFuture();
    mockFuture.completeExceptionally(new Throwable());
    when(pipeline.cancel(job)).thenReturn(mockFuture);
    pipelineManager.getOrCreatePipeline(pipelineFactory.getPipelineId(job), job);
    completedFuture = pipelineManager.cancel(job);
    Assert.assertTrue(completedFuture.toCompletableFuture().isDone());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCancelled());
    Assert.assertTrue(completedFuture.toCompletableFuture().isCompletedExceptionally());
    Assert.assertEquals(1, pipelineManager.getPipelines().size());
  }

  @Test
  public void testUpdate() {
    CompletionStage<Void> completedFuture = pipelineManager.update(job);
    Assert.assertTrue(completedFuture.toCompletableFuture().isDone());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCancelled());
    Assert.assertTrue(completedFuture.toCompletableFuture().isCompletedExceptionally());
    Assert.assertEquals(0, pipelineManager.getPipelines().size());

    CompletableFuture mockFuture = CompletableFuture.completedFuture(null);
    when(pipeline.update(job)).thenReturn(mockFuture);
    pipelineManager.getOrCreatePipeline(pipelineFactory.getPipelineId(job), job);
    completedFuture = pipelineManager.update(job);
    Assert.assertTrue(completedFuture.toCompletableFuture().isDone());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCancelled());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCompletedExceptionally());
    Assert.assertEquals(1, pipelineManager.getPipelines().size());

    mockFuture = new CompletableFuture();
    mockFuture.completeExceptionally(new Throwable());
    when(pipeline.update(job)).thenReturn(mockFuture);
    pipelineManager.getOrCreatePipeline(pipelineFactory.getPipelineId(job), job);
    completedFuture = pipelineManager.update(job);
    Assert.assertTrue(completedFuture.toCompletableFuture().isDone());
    Assert.assertFalse(completedFuture.toCompletableFuture().isCancelled());
    Assert.assertTrue(completedFuture.toCompletableFuture().isCompletedExceptionally());
    Assert.assertEquals(1, pipelineManager.getPipelines().size());
  }

  @Test
  public void testCancelAll() {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());

    when(pipeline.cancelAll()).thenReturn(CompletableFuture.completedFuture(null));
    when(otherPipeline.cancelAll()).thenReturn(CompletableFuture.completedFuture(null));
    pipelineManager.cancelAll();
    verify(pipeline, times(1)).cancelAll();
    verify(otherPipeline, times(1)).cancelAll();
  }

  @Test
  public void testCancelAllWithException() {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());
    CompletableFuture future = new CompletableFuture();
    future.completeExceptionally(new RuntimeException());
    when(pipeline.cancelAll()).thenReturn(future);
    when(otherPipeline.cancelAll()).thenReturn(CompletableFuture.completedFuture(null));
    pipelineManager.cancelAll();
    verify(pipeline, times(1)).cancelAll();
    verify(otherPipeline, times(1)).cancelAll();
  }

  @Test
  public void testGetAll() {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());

    when(pipeline.getJobStatus())
        .thenReturn(ImmutableList.of(JobStatus.newBuilder().setJob(job).build()));
    when(otherPipeline.getJobStatus())
        .thenReturn(ImmutableList.of(JobStatus.newBuilder().setJob(otherJob).build()));
    Collection<JobStatus> jobStatusList = pipelineManager.getJobStatus();
    Assert.assertEquals(2, jobStatusList.size());
    verify(pipeline, times(1)).getJobStatus();
    verify(otherPipeline, times(1)).getJobStatus();
  }

  @Test
  public void testGetAllWithException() {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());

    Mockito.doThrow(new RuntimeException()).when(pipeline).getJobStatus();
    Mockito.when(otherPipeline.getJobStatus())
        .thenReturn(ImmutableList.of(JobStatus.newBuilder().setJob(otherJob).build()));
    Collection<JobStatus> jobStatusList = pipelineManager.getJobStatus();
    Assert.assertEquals(1, jobStatusList.size());
    verify(pipeline, times(1)).getJobStatus();
    verify(otherPipeline, times(1)).getJobStatus();
  }

  @Test
  public void testGetAllMap() throws Exception {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());

    when(pipeline.getJobStatus())
        .thenReturn(ImmutableList.of(JobStatus.newBuilder().setJob(job).build()));
    when(otherPipeline.getJobStatus())
        .thenReturn(ImmutableList.of(JobStatus.newBuilder().setJob(otherJob).build()));
  }

  // two pipelines, one is running and has running jobs; one is not running and does not have
  // running jobs
  @Test
  public void testGcPipelines1() throws Exception {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());
    when(pipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder().setJob(job).setState(JobState.JOB_STATE_CANCELED).build()));
    when(otherPipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder()
                    .setJob(otherJob)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build()));
    when(pipeline.isRunning()).thenReturn(false);
    when(otherPipeline.isRunning()).thenReturn(true);
    pipelineManager.gcPipelines();
    Assert.assertTrue(pipelineManager.getPipelines().isEmpty());
    verify(pipeline, Mockito.never()).stop();
    verify(otherPipeline, Mockito.times(1)).stop();
  }

  // two pipelines, one is running and has running jobs; one is not running and does not have
  // running jobs
  @Test
  public void testGcPipelines2() throws Exception {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());
    when(pipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder().setJob(job).setState(JobState.JOB_STATE_CANCELED).build()));
    when(otherPipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder()
                    .setJob(otherJob)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build()));
    when(pipeline.isRunning()).thenReturn(false);
    when(otherPipeline.isRunning()).thenReturn(true);
    when(pipeline.getJobs()).thenReturn(ImmutableSet.of(Job.getDefaultInstance()));
    when(otherPipeline.getJobs()).thenReturn(ImmutableSet.of(Job.getDefaultInstance()));
    pipelineManager.gcPipelines();
    Assert.assertEquals(1, pipelineManager.getPipelines().size());
    verify(pipeline, Mockito.never()).stop();
    verify(otherPipeline, Mockito.never()).stop();
  }

  // two pipelines, one is running but does not have running jobs; one is running but has
  // running jobs
  @Test
  public void testGcPipelines3() throws Exception {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());
    when(pipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder().setJob(job).setState(JobState.JOB_STATE_CANCELED).build()));
    when(otherPipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder()
                    .setJob(otherJob)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build()));
    when(pipeline.isRunning()).thenReturn(true);
    when(otherPipeline.isRunning()).thenReturn(false);
    pipelineManager.gcPipelines();
    Assert.assertEquals(0, pipelineManager.getPipelines().size());
    verify(pipeline, Mockito.times(1)).stop();
    verify(otherPipeline, Mockito.never()).stop();
  }

  @Test
  public void testGcPipelinesWithException1() throws Exception {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());

    doThrow(new RuntimeException()).when(pipeline).stop(); // gc should gracefully handle exception.
    when(pipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder().setJob(job).setState(JobState.JOB_STATE_CANCELED).build()));
    when(otherPipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder()
                    .setJob(otherJob)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build()));
    when(pipeline.isRunning()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(otherPipeline.isRunning()).thenReturn(true);

    pipelineManager.gcPipelines();
    Assert.assertTrue(pipelineManager.getPipelines().isEmpty());
    verify(pipeline, times(1)).stop();
    verify(otherPipeline, Mockito.times(1)).stop();
  }

  @Test
  public void testGcPipelinesWithException2() throws Exception {
    prepareTwoPipelines();
    Assert.assertEquals(2, pipelineManager.getPipelines().size());

    doThrow(new RuntimeException()).when(pipeline).stop(); // gc should gracefully handle exception.
    when(pipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder().setJob(job).setState(JobState.JOB_STATE_CANCELED).build()));
    when(otherPipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder()
                    .setJob(otherJob)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build()));
    when(pipeline.isRunning()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(otherPipeline.isRunning()).thenReturn(true);
    when(pipeline.getJobs()).thenReturn(ImmutableSet.of(Job.getDefaultInstance()));
    when(otherPipeline.getJobs()).thenReturn(ImmutableSet.of(Job.getDefaultInstance()));
    pipelineManager.gcPipelines();
    Assert.assertEquals(1, pipelineManager.getPipelines().size());
    verify(pipeline, times(1)).stop();
    verify(otherPipeline, Mockito.never()).stop();
  }

  @Test
  public void testLogAndMetrics() {
    prepareTwoPipelines();
    when(pipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder().setJob(job).setState(JobState.JOB_STATE_CANCELED).build()));
    when(otherPipeline.getJobStatus())
        .thenReturn(
            ImmutableList.of(
                JobStatus.newBuilder()
                    .setJob(otherJob)
                    .setState(JobState.JOB_STATE_RUNNING)
                    .build()));
    pipelineManager.logAndMetrics();
    verify(scope, times(1)).gauge(ArgumentMatchers.eq("job.state.count"));
    verify(scope, times(2)).gauge(ArgumentMatchers.eq("jobstatus.state.count"));
    verify(scope, times(1)).gauge(ArgumentMatchers.eq("running.count"));
    verify(scope, times(1)).gauge(ArgumentMatchers.eq("gc.count"));
    verify(gauge, times(2)).update(ArgumentMatchers.eq(0.0));
    verify(gauge, times(2)).update(ArgumentMatchers.eq(1.0));
    verify(gauge, times(1)).update(ArgumentMatchers.eq(2.0));
  }

  @Test
  public void testPublishMetrics() {
    pipelineManager.getOrCreatePipeline(PIPELINE_ID, job);
    pipelineManager.publishMetrics();
    Mockito.verify(pipeline, Mockito.times(1)).publishMetrics();
    Mockito.verify(scope, Mockito.times(1)).gauge("job.host");
    Mockito.verify(gauge, Mockito.times(1)).update(1.0);
  }

  private void prepareTwoPipelines() {
    otherJob = Job.newBuilder().setJobId(1).build();
    otherPipeline = mock(Pipeline.class);
    when(pipelineFactory.getPipelineId(otherJob)).thenReturn("otherPipelineId");
    when(pipelineFactory.createPipeline("otherPipelineId", otherJob)).thenReturn(otherPipeline);
    when(pipeline.run(job)).thenReturn(CompletableFuture.completedFuture(null));
    when(otherPipeline.run(otherJob)).thenReturn(CompletableFuture.completedFuture(null));
    pipelineManager.run(job);
    pipelineManager.run(otherJob);
  }
}
