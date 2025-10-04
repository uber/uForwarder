package com.uber.data.kafka.consumerproxy.worker.processor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.uber.data.kafka.consumer.DLQMetadata;
import com.uber.data.kafka.consumerproxy.config.ProcessorConfiguration;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherImpl;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherMessage;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcResponse;
import com.uber.data.kafka.consumerproxy.worker.filter.Filter;
import com.uber.data.kafka.consumerproxy.worker.filter.OriginalClusterFilter;
import com.uber.data.kafka.consumerproxy.worker.limiter.InflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.limiter.LongFixedInflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.limiter.VegasAdaptiveInflightLimiter;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.context.ContextManager;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.datatransfer.worker.common.TracedConsumerRecord;
import com.uber.data.kafka.datatransfer.worker.pipelines.KafkaPipelineIssue;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.jodah.failsafe.function.CheckedSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class ProcessorImplTest extends ProcessorTestBase {
  private ScheduledExecutorService executor;
  private Sink<DispatcherMessage, DispatcherResponse> dispatcher;
  private PipelineStateManager pipelineStateManager;
  private AckManager ackManager;
  private ProcessorImpl processor;
  private ArgumentCaptor<ItemAndJob<DispatcherMessage>> dispatcherMessageArgumentCaptor;
  private CoreInfra infra;
  private Gauge inflight;
  private Histogram endToEndLatency;
  private OutboundMessageLimiter.Builder outboundMessageLimiterBuilder;
  private OutboundMessageLimiter outboundMessageLimiter;
  private UnprocessedMessageManager.Builder UnprocessedMessageManagerBuilder;
  private MessageAckStatusManager.Builder messageAckStatusManagerBuilder;
  private ProcessorConfiguration config;
  private Filter filter;
  private RateLimiter rateLimiter;
  private MockedStatic<RateLimiter> staticContext;

  @BeforeEach
  public void setUp() throws Exception {
    setupRateLimier(0.0);
    Scope scope = Mockito.mock(Scope.class);
    Timer timer = Mockito.mock(Timer.class);
    Stopwatch stopwatch = Mockito.mock(Stopwatch.class);
    Counter counter = Mockito.mock(Counter.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Histogram histogram = Mockito.mock(Histogram.class);
    ContextManager contextManager = Mockito.mock(ContextManager.class);
    Mockito.when(scope.subScope(ArgumentMatchers.anyString())).thenReturn(scope);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    inflight = Mockito.mock(Gauge.class);
    Mockito.when(scope.gauge("processor.outbound-cache.size")).thenReturn(inflight);
    Mockito.when(scope.histogram(ArgumentMatchers.anyString(), ArgumentMatchers.any()))
        .thenReturn(histogram);
    endToEndLatency = Mockito.mock(Histogram.class);
    Mockito.when(scope.histogram(Mockito.eq("message.e2e.latency"), Mockito.any()))
        .thenReturn(endToEndLatency);
    Mockito.when(timer.start()).thenReturn(stopwatch);
    infra = CoreInfra.builder().withScope(scope).build();
    executor = Executors.newSingleThreadScheduledExecutor();
    dispatcher = Mockito.mock(Sink.class);
    ackManager = Mockito.mock(AckManager.class);
    pipelineStateManager = Mockito.mock(PipelineStateManager.class);
    Mockito.when(pipelineStateManager.getFlowControl())
        .thenReturn(FlowControl.newBuilder().setMessagesPerSec(1000).setBytesPerSec(1000).build());
    Mockito.when(pipelineStateManager.getExpectedJob(100)).thenReturn(Optional.of(job));
    Mockito.when(contextManager.wrap(Mockito.any(ExecutorService.class)))
        .thenAnswer(
            (Answer<ExecutorService>)
                invocation -> invocation.getArgument(0, ExecutorService.class));
    Mockito.when(contextManager.wrap(Mockito.any(CheckedSupplier.class)))
        .thenAnswer(
            (Answer<CheckedSupplier>)
                invocation -> invocation.getArgument(0, CheckedSupplier.class));
    Mockito.when(contextManager.runAsync(Mockito.any(Runnable.class), Mockito.any(Executor.class)))
        .thenAnswer(
            (Answer<CompletableFuture>)
                invocation -> {
                  Runnable runnable = invocation.getArgument(0, Runnable.class);
                  Executor executor = invocation.getArgument(1, Executor.class);
                  return CompletableFuture.runAsync(runnable, executor);
                });
    outboundMessageLimiterBuilder =
        new SimpleOutboundMessageLimiter.Builder(
            infra, VegasAdaptiveInflightLimiter.newBuilder(), false);
    outboundMessageLimiter = Mockito.spy(outboundMessageLimiterBuilder.build(job));

    config = new ProcessorConfiguration();
    config.setMaxInboundCacheCount(1);
    config.setMaxInboundCacheByteSize(10);
    UnprocessedMessageManagerBuilder =
        new UnprocessedMessageManager.Builder(
            config, Mockito.mock(LongFixedInflightLimiter.class), infra);
    messageAckStatusManagerBuilder = new MessageAckStatusManager.Builder(1, infra);
    filter = new OriginalClusterFilter();
    processor =
        new ProcessorImpl(
            ackManager,
            executor,
            outboundMessageLimiter,
            job.getRpcDispatcherTask().getUri(),
            10,
            filter,
            1,
            1,
            infra);
    processor.setNextStage(dispatcher);
    processor.setPipelineStateManager(pipelineStateManager);

    // update the quota
    processor.run(job).toCompletableFuture().get();
    Mockito.when(ackManager.ack(Mockito.any(ProcessorMessage.class)))
        .thenReturn(ProcessorTestBase.OFFSET);
    Mockito.when(ackManager.nack(processorMessage.getPhysicalMetadata())).thenReturn(true);
    dispatcherMessageArgumentCaptor = ArgumentCaptor.forClass(ItemAndJob.class);
  }

  @AfterEach
  public void tearDown() {
    staticContext.close();
  }

  @SuppressWarnings({"CheckReturnValue"})
  private void setupRateLimier(double returnValue) {
    rateLimiter = Mockito.mock(RateLimiter.class);
    Mockito.doReturn(0.0).when(rateLimiter).acquire();
    Mockito.doReturn(0.0).when(rateLimiter).acquire(Mockito.anyInt());
    staticContext = Mockito.mockStatic(RateLimiter.class);
    staticContext.when(() -> RateLimiter.create(Mockito.anyDouble())).thenReturn(rateLimiter);
  }

  @Test
  public void testQuotaLimit() {
    Mockito.verify(rateLimiter, Mockito.times(2)).setRate(1000);
  }

  @Test
  public void testCreation() {
    new ProcessorImpl(
        job,
        executor,
        outboundMessageLimiterBuilder,
        messageAckStatusManagerBuilder,
        UnprocessedMessageManagerBuilder,
        filter,
        1,
        infra);
  }

  @Test
  public void testStartAndClose() {
    Assertions.assertFalse(processor.isRunning());
    processor.start();
    Assertions.assertTrue(processor.isRunning());
    processor.stop();
    Assertions.assertFalse(processor.isRunning());

    Mockito.doThrow(new RuntimeException()).when(ackManager).cancelAll();
    RuntimeException exception = null;
    try {
      processor.stop();
    } catch (RuntimeException e) {
      exception = e;
    }
    Assertions.assertNotNull(exception);
  }

  @Test
  public void testRun() {
    processor.run(job);
  }

  @Test
  public void testUpdate() {
    processor.update(job);
  }

  @Test
  public void testCancel() {
    processor.cancel(job);
  }

  @Test
  public void testCancelAll() {
    processor.cancelAll();
  }

  @Test
  public void testSubmitWithException() throws InterruptedException {
    assertThrows(
        ExecutionException.class,
        () -> {
          processor.start();
          Mockito.when(infra.scope().counter(ArgumentMatchers.anyString()))
              .thenThrow(new RuntimeException());
          CompletionStage<Long> offsetFuture = processor.submit(ItemAndJob.of(consumerRecord, job));
          offsetFuture.toCompletableFuture().get();
        });
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithDifferentClusterShouldBeFiltered() throws Exception {
    processor.start();
    consumerRecord.headers().add("original_cluster", "wrong-cluster".getBytes());
    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());
    Mockito.verify(dispatcher, Mockito.times(0)).submit(Mockito.any());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithSameClusterShouldNotBeFiltered() throws Exception {
    processor.start();
    CompletableFuture<DispatcherResponse> dispatcherFuture = new CompletableFuture<>();
    dispatcherFuture.complete(new DispatcherResponse(DispatcherResponse.Code.COMMIT));
    Mockito.when(dispatcher.submit(Mockito.any())).thenReturn(dispatcherFuture);

    // Use "CLUSTER" to verify case insensitive string comparision
    consumerRecord.headers().add("cluster", "CLUSTER".getBytes(StandardCharsets.UTF_8));
    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns success so it should not retry anywhere.
    Mockito.verify(dispatcher, Mockito.times(1)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(1, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getValue().getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getValue().getItem().getDestination());
    Assertions.assertEquals(
        0, dispatcherMessageArgumentCaptor.getValue().getItem().getGrpcMessage().getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor.getValue().getItem().getGrpcMessage().getDispatchAttempt());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithCommitResponse() throws Exception {
    processor.start();
    CompletableFuture<DispatcherResponse> dispatcherFuture = new CompletableFuture<>();
    dispatcherFuture.complete(new DispatcherResponse(DispatcherResponse.Code.COMMIT));
    Mockito.when(dispatcher.submit(Mockito.any())).thenReturn(dispatcherFuture);

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns success so it should not retry anywhere.
    Mockito.verify(dispatcher, Mockito.times(1)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(1, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getValue().getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getValue().getItem().getDestination());
    Assertions.assertEquals(
        0, dispatcherMessageArgumentCaptor.getValue().getItem().getGrpcMessage().getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor.getValue().getItem().getGrpcMessage().getDispatchAttempt());
    Assertions.assertEquals(
        2,
        processor.dlqDispatchManager.getTokens(
            new TopicPartition(ProcessorTestBase.TOPIC, ProcessorTestBase.PARTITION)));
    Mockito.verify(endToEndLatency, Mockito.times(1)).recordDuration(ArgumentMatchers.any());
    processor.cancel(job).toCompletableFuture().get(); // wait for actual cancel to complete
    offsetFuture = processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(-1, (long) offsetFuture.get());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithCanceledFuture() {
    assertThrows(
        java.util.concurrent.CompletionException.class,
        () -> {
          // canceled message will not retry
          processor.start();
          CompletableFuture<DispatcherResponse> dispatcherFuture = new CompletableFuture<>();
          Mockito.when(dispatcher.submit(Mockito.any())).thenReturn(dispatcherFuture);
          CompletableFuture<Long> offsetFuture =
              processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
          try {
            dispatcherFuture.cancel(false);
            offsetFuture.join();
          } catch (Exception e) {
            Mockito.verify(dispatcher, Mockito.times(2))
                .submit(dispatcherMessageArgumentCaptor.capture());
            Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
            Mockito.verify(inflight, Mockito.never()).update(Mockito.anyDouble());
            processor.publishMetrics();
            Mockito.verify(inflight).update(0.0);
            throw e;
          }
        });
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitTwoMessagesWithCommitResponse() throws Exception {
    processor.start();
    CompletableFuture<DispatcherResponse> dispatcherFuture = new CompletableFuture<>();
    dispatcherFuture.complete(new DispatcherResponse(DispatcherResponse.Code.COMMIT));
    Mockito.when(dispatcher.submit(Mockito.any())).thenReturn(dispatcherFuture);

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());
    offsetFuture = processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns success so it should not retry anywhere.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(
        3,
        processor.dlqDispatchManager.getTokens(
            new TopicPartition(ProcessorTestBase.TOPIC, ProcessorTestBase.PARTITION)));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitWithEmptyRecord() throws Exception {
    processor.start();
    CompletableFuture<DispatcherResponse> dispatcherFuture = new CompletableFuture<>();
    dispatcherFuture.complete(new DispatcherResponse(DispatcherResponse.Code.COMMIT));
    Mockito.when(dispatcher.submit(Mockito.any())).thenReturn(dispatcherFuture);
    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(emptyConsumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns success so it should not retry anywhere.
    Mockito.verify(dispatcher, Mockito.times(1)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(
        2,
        processor.dlqDispatchManager.getTokens(
            new TopicPartition(ProcessorTestBase.TOPIC, ProcessorTestBase.PARTITION)));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithRetryResponse() throws Exception {
    processor.start();
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns RETRY.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.RETRY)))
        // second call to Kafka producer returns commit.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns RETRY so it should have been retried to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RETRY_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithRetryResponseForTieredRetry() throws Exception {
    job =
        Job.newBuilder(job)
            .setRpcDispatcherTask(
                RpcDispatcherTask.newBuilder()
                    .setUri(ProcessorTestBase.MUTTLEY_ROUTING_KEY)
                    .setDlqTopic(ProcessorTestBase.DLQ_TOPIC)
                    .setRpcTimeoutMs(1000)
                    .setMaxRpcTimeouts(1)
                    .build())
            .setRetryConfig(
                RetryConfig.newBuilder()
                    .addRetryQueues(
                        RetryQueue.newBuilder()
                            .setRetryQueueTopic("topic1__retry")
                            .setProcessingDelayMs(10)
                            .setMaxRetryCount(5)
                            .build())
                    .addRetryQueues(
                        RetryQueue.newBuilder()
                            .setRetryQueueTopic("topic2__retry")
                            .setProcessingDelayMs(20)
                            .setMaxRetryCount(5)
                            .build())
                    .setRetryEnabled(true)
                    .build())
            .build();

    processor.start();
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns RETRY.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.RETRY)))
        // second call to Kafka producer returns commit.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns RETRY so it should have been retried to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        "topic1__retry",
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithGrpcException() throws Exception {
    processor.start();
    // first call to gRPC endpoint throws exception
    Mockito.doThrow(new RuntimeException())
        // second call to gRPC endpoint returns COMMIT
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());

    testSubmitMessageWithGrpcExceptionMessageCheck();
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithGrpcException2() throws Exception {
    processor.start();
    CompletableFuture completableFuture = new CompletableFuture();
    completableFuture.completeExceptionally(new RuntimeException());

    // first call to gRPC endpoint throws exception
    Mockito.doReturn(completableFuture)
        // second call to gRPC endpoint returns COMMIT
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());

    testSubmitMessageWithGrpcExceptionMessageCheck();
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testPipelineStateManagerDoesNotContainTheJob() throws Exception {
    Mockito.when(pipelineStateManager.getExpectedJob(100)).thenReturn(Optional.empty());
    testSubmitMessageWithGrpcException();
  }

  private void testSubmitMessageWithGrpcExceptionMessageCheck()
      throws ExecutionException, InterruptedException {
    processor.start();
    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns RETRY so it should have been retried to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithInvalidResponse() throws Exception {
    processor.start();
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns INVALID.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.INVALID)))
        // second call send to gRPC succeeds.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns INVALID so we should have retried to gRPC.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    Assertions.assertEquals(
        1,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithInvalidResponseNotRunning() throws Exception {
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns INVALID.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.INVALID)))
        // second call send to gRPC succeeds.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(-1, (long) offsetFuture.get());

    // gRPC endpoint returns INVALID so we should have retried to gRPC.
    Mockito.verify(dispatcher, Mockito.times(1)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(1, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithBackoffResponse() throws Exception {
    processor.start();
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns INVALID.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.BACKOFF)))
        // second call send to gRPC succeeds.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns INVALID so we should have retried to gRPC.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RETRY_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getTimeoutCount());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
  }

  @Test
  public void testSubmitMessageWithDroppedResponse()
      throws ExecutionException, InterruptedException {
    processor.start();
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns INVALID.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.DROPPED)))
        // second call send to gRPC succeeds.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns INVALID so we should have retried to gRPC.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    Assertions.assertEquals(
        1,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithBackoffResponseNotRunning() throws Exception {
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns INVALID.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.BACKOFF)))
        // second call send to gRPC succeeds.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());
    // gRPC endpoint returns INVALID so we should have retried to gRPC.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    // GRPC
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    // RQ
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RETRY_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
  }

  @Test
  public void testHandleTimeoutForNonBackoffMessages() throws Exception {
    processorMessage = newEmptyProcessMessage(new TopicPartitionOffset(TOPIC, PARTITION, OFFSET));
    DispatcherResponse result =
        processor.handleTimeout(
            new DispatcherResponse(DispatcherResponse.Code.SKIP),
            processorMessage,
            Job.newBuilder().build());
    Assertions.assertEquals(DispatcherResponse.Code.SKIP, result.getCode());
    Assertions.assertEquals(0, processorMessage.getTimeoutCount());

    Job dlqJob = newJob(DLQ_TOPIC, PARTITION);
    DLQMetadata dlqMeta = DLQMetadata.newBuilder().setTimeoutCount(2).build();
    processorMessage = newRetryProcessMessage(dlqJob, OFFSET, dlqMeta);
    result =
        processor.handleTimeout(
            new DispatcherResponse(DispatcherResponse.Code.COMMIT),
            processorMessage,
            Job.newBuilder().build());
    Assertions.assertEquals(DispatcherResponse.Code.COMMIT, result.getCode());
    Assertions.assertEquals(2, processorMessage.getTimeoutCount());
  }

  @Test
  public void testHandleBackoffFromDlQ() throws Exception {
    Job dlqJob = newJob(DLQ_TOPIC, PARTITION);
    DLQMetadata dlqMeta = DLQMetadata.newBuilder().setTimeoutCount(2).build();
    processorMessage = newRetryProcessMessage(dlqJob, OFFSET, dlqMeta);
    DispatcherResponse result =
        processor.handleTimeout(
            new DispatcherResponse(DispatcherResponse.Code.BACKOFF), processorMessage, dlqJob);
    Assertions.assertEquals(DispatcherResponse.Code.DLQ, result.getCode());
    Assertions.assertEquals(3, processorMessage.getTimeoutCount());
  }

  @Test
  public void testHandleBackoffFromRESQ() throws Exception {
    Job resqJob = newJob(RESQ_TOPIC, PARTITION);
    DLQMetadata dlqMeta = DLQMetadata.newBuilder().setTimeoutCount(1).build();
    processorMessage = newRetryProcessMessage(resqJob, OFFSET, dlqMeta);
    DispatcherResponse result =
        processor.handleTimeout(
            new DispatcherResponse(DispatcherResponse.Code.BACKOFF), processorMessage, resqJob);
    Assertions.assertEquals(DispatcherResponse.Code.INVALID, result.getCode());
    Assertions.assertEquals(2, processorMessage.getTimeoutCount());
  }

  @Test
  public void testHandleCommitForBackoff() {
    TopicPartition topicPartition =
        new TopicPartition(ProcessorTestBase.TOPIC, ProcessorTestBase.PARTITION);
    int nToken = processor.dlqDispatchManager.getTokens(topicPartition);
    DispatcherResponse result =
        processor.handleTimeout(
            new DispatcherResponse(DispatcherResponse.Code.COMMIT), processorMessage, job);
    Assertions.assertEquals(DispatcherResponse.Code.COMMIT, result.getCode());
    Assertions.assertEquals(nToken + 1, processor.dlqDispatchManager.getTokens(topicPartition));
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithStashResponse() throws Exception {
    processor.start();
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns STASH.
        .thenReturn(
            CompletableFuture.completedFuture(new DispatcherResponse(DispatcherResponse.Code.DLQ)))
        // next call send to DLQ topic succeeds.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns STASH so we should have retried to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.DLQ_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithSkipResponse() throws Exception {
    processor.start();
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns SKIP.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.SKIP)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns SKIP so we commit this.
    Mockito.verify(dispatcher, Mockito.times(1)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(1, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
  }

  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithRetryResponseAndKafkaProduceFailure() throws Exception {
    processor.start();
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns RETRY.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.RETRY)))
        // second call to Kafka Producer fails
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.INVALID)))
        // third call to Kafka Producer fails
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns RETRY so we sent to KAFKA.
    // Kafka send then returns INVALID, so we should retry to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(3)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(3, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RETRY_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(2).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RETRY_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(2).getItem().getDestination());
    dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(2)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
  }

  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithRetryResponseAndKafkaProduceFailureNotRunning()
      throws Exception {
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns RETRY.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.RETRY)))
        // second call to Kafka Producer fails
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.INVALID)))
        // third call to Kafka Producer fails
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(-1, (long) offsetFuture.get());

    // gRPC endpoint returns RETRY so we sent to KAFKA.
    // Kafka send then returns INVALID, so we should retry to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RETRY_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
  }

  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithRetryResponseAndKafkaProduceException() throws Exception {
    processor.start();
    Mockito.doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.RETRY)))
        .doThrow(new RuntimeException())
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());

    testSubmitMessageWithRetryResponseAndKafkaProduceExceptionMessageCheck();
  }

  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithRetryResponseAndKafkaProduceException2() throws Exception {
    processor.start();
    CompletableFuture completableFuture = new CompletableFuture();
    completableFuture.completeExceptionally(new RuntimeException());

    Mockito.doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.RETRY)))
        .doReturn(completableFuture)
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());

    testSubmitMessageWithRetryResponseAndKafkaProduceExceptionMessageCheck();
  }

  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitMessageWithResqResponseAndKafkaProduceException() throws Exception {
    processor.start();
    CompletableFuture completableFuture = new CompletableFuture();
    completableFuture.completeExceptionally(new RuntimeException());

    Mockito.doReturn(
            CompletableFuture.completedFuture(new DispatcherResponse(DispatcherResponse.Code.RESQ)))
        .doReturn(completableFuture)
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());
    // gRPC endpoint returns RETRY so we sent to KAFKA.
    // Kafka send then returns INVALID, so we should retry to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(3)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(3, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RESQ_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(2).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RESQ_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(2).getItem().getDestination());
    dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(2)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
  }

  private void testSubmitMessageWithRetryResponseAndKafkaProduceExceptionMessageCheck()
      throws InvalidProtocolBufferException, ExecutionException, InterruptedException {
    processor.start();
    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());
    // gRPC endpoint returns RETRY so we sent to KAFKA.
    // Kafka send then returns INVALID, so we should retry to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(3)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(3, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RETRY_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(2).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RETRY_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(2).getItem().getDestination());
    dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(2)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
  }

  @Test
  public void testGrpcDispatcherAlwaysFailed() throws InterruptedException {
    processor.start();
    CompletableFuture completableFuture = new CompletableFuture();
    completableFuture.completeExceptionally(new RuntimeException());

    Mockito.doReturn(completableFuture)
        .doReturn(completableFuture)
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());
    ExecutionException exception = null;
    try {
      processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture().get();
    } catch (ExecutionException e) {
      exception = e;
    }
    Assertions.assertNotNull(exception);
  }

  @Test
  public void testGrpcDispatcherAlwaysFailed2() throws InterruptedException {
    processor.start();
    Mockito.doThrow(new RuntimeException())
        .doThrow(new RuntimeException())
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());
    ExecutionException exception = null;
    try {
      processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture().get();
    } catch (ExecutionException e) {
      exception = e;
    }
    Assertions.assertNotNull(exception);
  }

  @Test
  public void testGrpcDispatcherNotCalledAfterCancelJob() throws InterruptedException {
    processor.start();
    processor.cancel(job).toCompletableFuture().join();
    ExecutionException exception = null;
    try {
      processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture().get();
    } catch (ExecutionException e) {
      exception = e;
    }
    // after job cancel, message should be skipped without being dispatched
    Mockito.verify(dispatcher, Mockito.never()).submit(Mockito.any());
    Assertions.assertNull(exception);
  }

  @Test
  public void testGrpcDispatcherReturnInvalidAndFailure() throws InterruptedException {
    processor.start();
    Mockito.doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.INVALID)))
        .doThrow(new RuntimeException())
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());
    ExecutionException exception = null;
    try {
      processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture().get();
    } catch (ExecutionException e) {
      exception = e;
    }
    Assertions.assertNotNull(exception);
  }

  @Test
  public void testGrpcDispatcherReturnInvalidAndFailureNotRunning()
      throws InterruptedException, ExecutionException {
    Mockito.doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.INVALID)))
        .doThrow(new RuntimeException())
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());
    long offset = processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture().get();
    Assertions.assertEquals(-1, offset);
  }

  @Test
  public void testGrpcDispatcherAlwaysReturnInvalid()
      throws InterruptedException, ExecutionException {
    processor.start();
    Mockito.doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.INVALID)))
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.INVALID)))
        .doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(-1, (long) offsetFuture.get());
  }

  @Test
  public void testEmptyDLQTopicFallbackToRetryTopic()
      throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
    processor.start();
    Job.Builder builder = Job.newBuilder(job);
    builder.getRpcDispatcherTaskBuilder().clearDlqTopic();
    Job jobWithoutDLQTopic = builder.build();

    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns STASH.
        .thenReturn(
            CompletableFuture.completedFuture(new DispatcherResponse(DispatcherResponse.Code.DLQ)))
        // next call send to DLQ topic succeeds.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, jobWithoutDLQTopic)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns RETRY so it should have been retried to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.RETRY_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(1, dlqMetadata.getRetryCount());
  }

  @Test
  public void testEmptyRetryTopicNotFallbackToResilienceTopic()
      throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
    processor.start();
    Job.Builder builder = Job.newBuilder(job);
    builder.clearRetryConfig();
    Job jobWithoutRetryTopic = builder.build();

    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns STASH.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.RETRY)))
        // next call send to DLQ topic succeeds.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, jobWithoutRetryTopic)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns RETRY so it should have been retried to KAFKA.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    Assertions.assertEquals(
        1,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        1,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
  }

  @Test
  public void testEmptyDLQTopicAndRetryQueueTopicFallbackToRetry()
      throws ExecutionException, InterruptedException {
    processor.start();
    Job.Builder builder = Job.newBuilder(job);
    builder.getRpcDispatcherTaskBuilder().clearRetryQueueTopic().clearDlqTopic();
    builder.clearRetryConfig();
    builder.clearResqConfig();
    Job jobWithoutDLQAndRetryQueueTopic = builder.build();

    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns STASH.
        .thenReturn(
            CompletableFuture.completedFuture(new DispatcherResponse(DispatcherResponse.Code.DLQ)))
        // next call to gRPC endpoint returns COMMIT.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor
            .submit(ItemAndJob.of(consumerRecord, jobWithoutDLQAndRetryQueueTopic))
            .toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns STASH but dlq topic and retry queue topic are empty so it should have
    // been resent to GRPC.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    Assertions.assertEquals(
        1,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        1,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
  }

  @Test
  public void testEmptyDLQTopicAndRetryQueueTopicFallbackToRetryNotRunning()
      throws ExecutionException, InterruptedException {
    Job.Builder builder = Job.newBuilder(job);
    builder.getRpcDispatcherTaskBuilder().clearRetryQueueTopic().clearDlqTopic();
    builder.clearRetryConfig();
    builder.clearResqConfig();
    Job jobWithoutDLQAndRetryQueueTopic = builder.build();

    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns STASH.
        .thenReturn(
            CompletableFuture.completedFuture(new DispatcherResponse(DispatcherResponse.Code.DLQ)));

    CompletableFuture<Long> offsetFuture =
        processor
            .submit(ItemAndJob.of(consumerRecord, jobWithoutDLQAndRetryQueueTopic))
            .toCompletableFuture();
    Assertions.assertEquals(-1, (long) offsetFuture.get());

    // gRPC endpoint returns STASH but dlq topic and retry queue topic are empty so it should have
    // been resent to GRPC. However, the processor is not running, so no more action.
    Mockito.verify(dispatcher, Mockito.times(1)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(1, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testEmptyRetryQueueTopicFallbackToRetry()
      throws ExecutionException, InterruptedException {
    processor.start();
    Job.Builder builder = Job.newBuilder(job);
    builder.clearRetryConfig();
    builder.clearResqConfig();
    Job jobWithoutRetryQueueTopic = builder.build();

    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns RETRY.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.RETRY)))
        // second call to gRPC endpoint returns commit.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor
            .submit(ItemAndJob.of(consumerRecord, jobWithoutRetryQueueTopic))
            .toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns RETRY but retry queue topic is empty so it should have been resent to
    // GRPC.
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    Assertions.assertEquals(
        1,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        1,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(1)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
    Mockito.verify(pipelineStateManager, Mockito.times(1))
        .reportIssue(
            jobWithoutRetryQueueTopic,
            KafkaPipelineIssue.RETRY_WITHOUT_RETRY_QUEUE.getPipelineHealthIssue());
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testEmptyRetryQueueTopicFallbackToRetryNotRunning()
      throws ExecutionException, InterruptedException {
    Job.Builder builder = Job.newBuilder(job);
    builder.clearRetryConfig();
    builder.clearResqConfig();
    builder.getRpcDispatcherTaskBuilder().clearDlqTopic();
    Job jobWithoutRetryQueueTopic = builder.build();

    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns RETRY.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.RETRY)));

    CompletableFuture<Long> offsetFuture =
        processor
            .submit(ItemAndJob.of(consumerRecord, jobWithoutRetryQueueTopic))
            .toCompletableFuture();
    Assertions.assertEquals(-1, (long) offsetFuture.get());

    // gRPC endpoint returns RETRY but retry queue topic is empty so it should have been resent to
    // GRPC. However, the processor is not running so no more actions.
    Mockito.verify(dispatcher, Mockito.times(1)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(1, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());
  }

  @Test
  public void testProcessTracedConsumerRecord() throws InterruptedException {
    TracedConsumerRecord tracedConsumerRecord =
        TracedConsumerRecord.of(consumerRecord, infra.tracer(), "consumerGroup");
    Assertions.assertTrue(tracedConsumerRecord.span().isPresent());
    processor.start();
    Mockito.doReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)))
        .when(dispatcher)
        .submit(Mockito.any());
    ExecutionException exception = null;
    try {
      processor.submit(ItemAndJob.of(tracedConsumerRecord, job)).toCompletableFuture().get();
    } catch (ExecutionException e) {
      exception = e;
    }
    Assertions.assertNull(exception);
  }

  @Test
  public void testPublishMetrics() {
    processor.publishMetrics();
    Mockito.verify(ackManager, Mockito.times(1)).publishMetrics();
  }

  @Test
  public void testMaxRpcTimeoutsAndDLQ()
      throws InvalidProtocolBufferException, ExecutionException, InterruptedException {

    // we want to test message enters DLQ when timeoit count >= maxRpcTimeouts. Previously
    // BACKOFF used to do in-memory retry. With change to BACKOFF behavior, we are creating mock
    // with timeout=1, such that on next attempt the message will be sent to DLQ.
    DLQMetadata dlqMeta = DLQMetadata.newBuilder().setTimeoutCount(1).setRetryCount(1).build();
    consumerRecord =
        new ConsumerRecord<>(
            ProcessorTestBase.RETRY_TOPIC,
            ProcessorTestBase.RETRY_PARTITION,
            ProcessorTestBase.OFFSET,
            ProcessorTestBase.TIMESTAMP,
            ProcessorTestBase.TIMESTAMP_TYPE,
            null,
            dlqMeta.toByteArray().length,
            ProcessorTestBase.VALUE.length(),
            dlqMeta.toByteArray(),
            ProcessorTestBase.VALUE.getBytes(),
            headers);
    processor.run(retryJob).toCompletableFuture().get();
    processor.start();
    Mockito.when(dispatcher.submit(Mockito.any()))
        // first call to gRPC endpoint returns BACKOFF.
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.BACKOFF)))
        .thenReturn(
            CompletableFuture.completedFuture(
                new DispatcherResponse(DispatcherResponse.Code.COMMIT)));

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    // gRPC endpoint returns BACKOFF twice so we should have retried to RPC endpoint twice.
    // finally retried to STASH when exceeds maxRpcTimeouts;
    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        1,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getDispatchAttempt());

    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.DLQ_TOPIC,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata1 =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(2, dlqMetadata1.getRetryCount());
  }

  @Test
  public void testControlFlowNegativeValue() {
    Mockito.when(pipelineStateManager.getFlowControl())
        .thenReturn(
            FlowControl.newBuilder()
                .setMessagesPerSec(1000)
                .setBytesPerSec(1000)
                .setMaxInflightMessages(-2)
                .build());
    Exception exception =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> {
              processor.run(job).toCompletableFuture().get();
              processor.start();
            });
    String expectedMessage = "The inflight message limit is not valid";
    Assertions.assertTrue(exception.getMessage().contains(expectedMessage));
  }

  @Test
  public void testControlFlowTooLargeValue() {
    Mockito.when(pipelineStateManager.getFlowControl())
        .thenReturn(
            FlowControl.newBuilder()
                .setMessagesPerSec(1000)
                .setBytesPerSec(1000)
                .setMaxInflightMessages((double) Integer.MAX_VALUE + 1)
                .build());
    Exception exception =
        Assertions.assertThrows(
            ExecutionException.class,
            () -> {
              processor.run(job).toCompletableFuture().get();
              processor.start();
            });
    String expectedMessage = "The inflight message limit is not valid";
    Assertions.assertTrue(exception.getMessage().contains(expectedMessage));
  }

  @Test
  public void testCancelProcessWithRetryRPCFailed()
      throws InvalidProtocolBufferException, ExecutionException, InterruptedException {
    testCancelProcessWithCode(DispatcherResponse.Code.RETRY, false, RETRY_TOPIC, 1);
  }

  @Test
  public void testCancelProcessWithDLQ()
      throws InvalidProtocolBufferException, ExecutionException, InterruptedException {
    testCancelProcessWithCode(DispatcherResponse.Code.DLQ, true, DLQ_TOPIC, 0);
  }

  @Test
  public void testCancelSafeStage() throws ExecutionException, InterruptedException {
    final CompletableFuture<Integer> future = new CompletableFuture<>();
    ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.schedule(
        () -> {
          future.complete(10);
        },
        1,
        TimeUnit.SECONDS);
    ProcessorImpl.cancelSafeStage(future).toCompletableFuture().cancel(false);
    Assertions.assertEquals(10, future.get().intValue());
  }

  private void testCancelProcessWithCode(
      DispatcherResponse.Code cancelCode,
      boolean cancelRetry,
      String kafkaDispatchTopic,
      int retryCount)
      throws InvalidProtocolBufferException, ExecutionException, InterruptedException {
    processor.start();
    AtomicInteger count = new AtomicInteger(0);
    Mockito.when(dispatcher.submit(Mockito.any()))
        .thenAnswer(
            (Answer<CompletionStage>)
                invocation -> {
                  int attempts = count.getAndIncrement();
                  if (attempts == 0) {
                    ArgumentCaptor<ProcessorMessage> messageCaptor =
                        ArgumentCaptor.forClass(ProcessorMessage.class);
                    Mockito.verify(ackManager, Mockito.times(1)).receive(messageCaptor.capture());
                    ProcessorMessage pm = messageCaptor.getValue();
                    CompletionStage<GrpcResponse> rpcDispatcherFuture =
                        CompletableFuture.completedFuture(GrpcResponse.of(Status.UNAVAILABLE));
                    MessageStub.Attempt attempt = pm.getStub().newAttempt();
                    if (cancelRetry) {
                      // grpc finished before cancel, will cancel retry
                      rpcDispatcherFuture = attempt.complete(rpcDispatcherFuture);
                      pm.getStub().cancel(cancelCode);
                    } else {
                      // cancel before grpc finish, will cancel grpc
                      pm.getStub().cancel(cancelCode);
                      rpcDispatcherFuture = attempt.complete(rpcDispatcherFuture);
                    }
                    return rpcDispatcherFuture.thenApply(
                        grpcResponse ->
                            DispatcherImpl.dispatcherResponseFromGrpcStatus(grpcResponse));
                  } else if (attempts == 1) {
                    return CompletableFuture.completedFuture(
                        new DispatcherResponse(DispatcherResponse.Code.COMMIT));
                  } else throw new IllegalStateException();
                });
    processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture().get();

    Mockito.verify(dispatcher, Mockito.times(2)).submit(dispatcherMessageArgumentCaptor.capture());
    Assertions.assertEquals(2, dispatcherMessageArgumentCaptor.getAllValues().size());
    Assertions.assertEquals(
        DispatcherMessage.Type.GRPC,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getType());
    Assertions.assertEquals(
        ProcessorTestBase.MUTTLEY_ROUTING_KEY,
        dispatcherMessageArgumentCaptor.getAllValues().get(0).getItem().getDestination());
    Assertions.assertEquals(
        0,
        dispatcherMessageArgumentCaptor
            .getAllValues()
            .get(0)
            .getItem()
            .getGrpcMessage()
            .getRetryCount());
    Assertions.assertEquals(
        DispatcherMessage.Type.KAFKA,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getType());
    Assertions.assertEquals(
        kafkaDispatchTopic,
        dispatcherMessageArgumentCaptor.getAllValues().get(1).getItem().getDestination());
    DLQMetadata dlqMetadata =
        DLQMetadata.parseFrom(
            dispatcherMessageArgumentCaptor
                .getAllValues()
                .get(1)
                .getItem()
                .getProducerRecord()
                .key());
    Assertions.assertEquals(retryCount, dlqMetadata.getRetryCount());
  }

  @Test
  public void testGetStubs() {
    processor.getStubs();
    Mockito.verify(ackManager, Mockito.times(1)).getStubs();
  }

  @Test
  public void testGetMetricsTags() {
    Map<String, String> tags = processor.getMetricsTags(job);
    Assertions.assertEquals("routing-key", tags.get("uri"));
    Assertions.assertEquals(ProcessorTestBase.TOPIC, tags.get("kafka_topic"));
    Assertions.assertEquals(ProcessorTestBase.GROUP, tags.get("kafka_group"));
    Assertions.assertEquals(ProcessorTestBase.CLUSTER, tags.get("kafka_cluster"));
    Assertions.assertEquals(
        Integer.toString(ProcessorTestBase.PARTITION), tags.get("kafka_partition"));
    Assertions.assertEquals(ProcessorTestBase.CONSUMER_SERVICE_NAME, tags.get("consumer_service"));
  }

  @Test
  public void testHandlePermit() {
    InflightLimiter.Permit permit = Mockito.mock(InflightLimiter.Permit.class);
    processor.handlePermit(new DispatcherResponse(DispatcherResponse.Code.BACKOFF), null, permit);
    processor.handlePermit(new DispatcherResponse(DispatcherResponse.Code.INVALID), null, permit);
    Mockito.verify(permit, Mockito.times(2)).complete(InflightLimiter.Result.Failed);

    Mockito.reset(permit);
    processor.handlePermit(new DispatcherResponse(DispatcherResponse.Code.RESQ), null, permit);
    processor.handlePermit(new DispatcherResponse(DispatcherResponse.Code.COMMIT), null, permit);
    processor.handlePermit(new DispatcherResponse(DispatcherResponse.Code.SKIP), null, permit);
    processor.handlePermit(new DispatcherResponse(DispatcherResponse.Code.RETRY), null, permit);
    processor.handlePermit(new DispatcherResponse(DispatcherResponse.Code.DLQ), null, permit);
    Mockito.verify(permit, Mockito.times(5)).complete(InflightLimiter.Result.Succeed);

    Mockito.reset(permit);
    processor.handlePermit(new DispatcherResponse(DispatcherResponse.Code.DROPPED), null, permit);
    Mockito.verify(permit, Mockito.times(1)).complete(InflightLimiter.Result.Dropped);

    Mockito.reset(permit);
    processor.handlePermit(
        new DispatcherResponse(DispatcherResponse.Code.COMMIT), new RuntimeException(), permit);
    Mockito.verify(permit, Mockito.times(1)).complete();
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testSubmitRateLimited() throws Exception {
    Mockito.doReturn(1.0).when(rateLimiter).acquire();
    Mockito.doReturn(1.0).when(rateLimiter).acquire(Mockito.anyInt());
    processor.start();
    CompletableFuture<DispatcherResponse> dispatcherFuture = new CompletableFuture<>();
    dispatcherFuture.complete(new DispatcherResponse(DispatcherResponse.Code.COMMIT));
    Mockito.when(dispatcher.submit(Mockito.any())).thenReturn(dispatcherFuture);

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    Mockito.verify(pipelineStateManager, Mockito.times(1))
        .reportIssue(job, KafkaPipelineIssue.MESSAGE_RATE_LIMITED.getPipelineHealthIssue());
    Mockito.verify(pipelineStateManager, Mockito.times(1))
        .reportIssue(job, KafkaPipelineIssue.BYTES_RATE_LIMITED.getPipelineHealthIssue());
  }

  @Test
  public void testReportInflightLimitedIssue() throws ExecutionException, InterruptedException {
    Mockito.when(outboundMessageLimiter.getStats())
        .thenReturn(new SimpleOutboundMessageLimiter.StatsImpl(0.9));

    processor.start();
    CompletableFuture<DispatcherResponse> dispatcherFuture = new CompletableFuture<>();
    dispatcherFuture.complete(new DispatcherResponse(DispatcherResponse.Code.COMMIT));
    Mockito.when(dispatcher.submit(Mockito.any())).thenReturn(dispatcherFuture);

    CompletableFuture<Long> offsetFuture =
        processor.submit(ItemAndJob.of(consumerRecord, job)).toCompletableFuture();
    Assertions.assertEquals(ProcessorTestBase.OFFSET, (long) offsetFuture.get());

    Mockito.verify(pipelineStateManager, Mockito.times(1))
        .reportIssue(job, KafkaPipelineIssue.INFLIGHT_MESSAGE_LIMITED.getPipelineHealthIssue());
  }
}
