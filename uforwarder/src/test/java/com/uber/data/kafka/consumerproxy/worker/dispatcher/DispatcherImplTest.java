package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcDispatcher;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcResponse;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.ResqConfig;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.dispatchers.kafka.KafkaDispatcher;
import com.uber.data.kafka.datatransfer.worker.pipelines.KafkaPipelineIssue;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;
import io.grpc.Status;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class DispatcherImplTest {
  private static final String RESQ_TOPIC = "topic__resq";
  private GrpcDispatcher grpcDispatcher;
  private KafkaDispatcher dlqProducer;
  private KafkaDispatcher resqProducer;
  private Job job;
  private Job jobWithInvalidTimeout;
  private CoreInfra coreInfra;
  private Counter counter;
  private Timer timer;
  private DispatcherImpl dispatcher;
  private DispatcherMessage grpcDispatcherMessage;
  private DispatcherMessage kafkaDispatcherMessage;
  private DispatcherMessage kafkaResqDispatcherMessage;
  private Headers headers;
  private PipelineStateManager pipelineStateManager;

  private LatencyTracker latencyTracker;

  @BeforeEach
  public void setUp() {
    headers = new RecordHeaders();
    grpcDispatcher = Mockito.mock(GrpcDispatcher.class);
    dlqProducer = Mockito.mock(KafkaDispatcher.class);
    resqProducer = Mockito.mock(KafkaDispatcher.class);
    job =
        Job.newBuilder()
            .setJobId(1)
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().setRpcTimeoutMs(10).build())
            .setResqConfig(ResqConfig.newBuilder().setResqTopic(RESQ_TOPIC).build())
            .build();
    jobWithInvalidTimeout = Job.newBuilder().build();
    Scope scope = Mockito.mock(Scope.class);
    coreInfra = CoreInfra.builder().withScope(scope).build();
    counter = Mockito.mock(Counter.class);
    timer = Mockito.mock(Timer.class);
    Mockito.when(scope.subScope(ArgumentMatchers.anyString())).thenReturn(scope);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);

    pipelineStateManager = Mockito.mock(PipelineStateManager.class);
    latencyTracker = Mockito.mock(LatencyTracker.class);
    Mockito.when(latencyTracker.startSpan())
        .thenReturn(Mockito.mock(LatencyTracker.LatencySpan.class));
    Mockito.when(latencyTracker.getStats())
        .thenReturn(new LatencyTracker.Stats(1000, 100, 200, 200, 2000));
    dispatcher =
        new DispatcherImpl(
            coreInfra, grpcDispatcher, dlqProducer, Optional.of(resqProducer), latencyTracker);
    dispatcher.setPipelineStateManager(pipelineStateManager);
    MessageStub mockStub = Mockito.mock(MessageStub.class);
    grpcDispatcherMessage =
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            "muttley://foo",
            "key".getBytes(),
            "value".getBytes(),
            headers,
            "group",
            "topic",
            0,
            0,
            mockStub,
            "physicaltopic",
            "physicalCluster",
            0,
            0,
            0,
            0,
            0);
    kafkaDispatcherMessage =
        new DispatcherMessage(
            DispatcherMessage.Type.KAFKA,
            "topic__dlq",
            "key".getBytes(),
            "value".getBytes(),
            headers,
            "group",
            "topic",
            0,
            0,
            mockStub,
            "physicaltopic",
            "physicalCluster",
            0,
            0,
            0,
            0,
            0);
    kafkaResqDispatcherMessage =
        new DispatcherMessage(
            DispatcherMessage.Type.KAFKA,
            RESQ_TOPIC,
            "key".getBytes(),
            "value".getBytes(),
            headers,
            "group",
            "topic",
            0,
            0,
            mockStub,
            "physicaltopic",
            "physicalCluster",
            0,
            0,
            0,
            0,
            0);
  }

  @Test
  public void testStartAndClose() throws Exception {
    Assertions.assertFalse(dispatcher.isRunning());
    dispatcher.start();
    Assertions.assertTrue(dispatcher.isRunning());
    dispatcher.stop();
    Assertions.assertFalse(dispatcher.isRunning());
    Mockito.verify(resqProducer, Mockito.times(1)).stop();
    // close one more time
    dispatcher.stop();
    Assertions.assertFalse(dispatcher.isRunning());

    Mockito.doThrow(new RuntimeException()).when(grpcDispatcher).stop();
    Mockito.when(grpcDispatcher.isRunning()).thenReturn(true);
    RuntimeException exception = null;
    try {
      dispatcher.stop();
    } catch (RuntimeException e) {
      exception = e;
    }
    Assertions.assertNotNull(exception);
  }

  @Test
  public void testSubmitGrpcMessage() throws Exception {
    // Test status code propagation
    // We have a separate unit test for the mapping from grpc status code to dispatcher response
    // code.
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(
            CompletableFuture.completedFuture(GrpcResponse.of(Status.fromCode(Status.Code.OK))));
    Assertions.assertEquals(
        DispatcherResponse.Code.COMMIT,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());

    // Test exceptional completion is propagated up.
    CompletableFuture<GrpcResponse> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException());
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(future);
    Assertions.assertTrue(
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).isCompletedExceptionally());
  }

  @Test
  public void testSubmitGrpcMessageWithResponseCode() throws Exception {
    // Test status code propagation
    // We have a separate unit test for the mapping from grpc status code to dispatcher response
    // code.
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(
            CompletableFuture.completedFuture(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.DEADLINE_EXCEEDED), DispatcherResponse.Code.DLQ)));
    Assertions.assertEquals(
        DispatcherResponse.Code.DLQ,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());
  }

  @Test
  public void testSubmitKafkaMessage() throws Exception {
    // Test that null return on CompletableFuture<Void> is translated to COMMIT response code
    Mockito.when(dlqProducer.submit(ItemAndJob.of(kafkaDispatcherMessage.getProducerRecord(), job)))
        .thenReturn(CompletableFuture.completedFuture(null));
    Assertions.assertEquals(
        DispatcherResponse.Code.COMMIT,
        dispatcher.submit(ItemAndJob.of(kafkaDispatcherMessage, job)).get().getCode());

    // Test exceptional completion is propagated up.
    CompletableFuture<DispatcherResponse> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException());
    Mockito.when(dlqProducer.submit(ItemAndJob.of(kafkaDispatcherMessage.getProducerRecord(), job)))
        .thenReturn(future);
    Assertions.assertTrue(
        dispatcher.submit(ItemAndJob.of(kafkaDispatcherMessage, job)).isCompletedExceptionally());
  }

  @Test
  public void testSubmitResqKafkaMessage() throws ExecutionException, InterruptedException {
    Mockito.when(
            resqProducer.submit(ItemAndJob.of(kafkaResqDispatcherMessage.getProducerRecord(), job)))
        .thenReturn(CompletableFuture.completedFuture(null));
    Assertions.assertEquals(
        DispatcherResponse.Code.COMMIT,
        dispatcher.submit(ItemAndJob.of(kafkaResqDispatcherMessage, job)).get().getCode());
  }

  @Test
  public void testSubmitResqKafkaMessageWithoutResqProducer() throws Throwable {
    assertThrows(
        IllegalStateException.class,
        () -> {
          DispatcherImpl badDispatcher =
              new DispatcherImpl(
                  coreInfra, grpcDispatcher, dlqProducer, Optional.empty(), latencyTracker);
          try {
            badDispatcher.submit(ItemAndJob.of(kafkaResqDispatcherMessage, job)).get();
          } catch (ExecutionException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  public void testSubmitGrpcMessageWithSkipResponseCode() throws Exception {
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(
            CompletableFuture.completedFuture(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.DEADLINE_EXCEEDED), DispatcherResponse.Code.SKIP)));
    Assertions.assertEquals(
        DispatcherResponse.Code.SKIP,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatusOverDue() {
    Assertions.assertEquals(
        DispatcherResponse.Code.BACKOFF,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(Status.fromCode(Status.Code.UNAVAILABLE), Optional.empty(), true))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeOk() {
    Assertions.assertEquals(
        DispatcherResponse.Code.COMMIT,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(Status.fromCode(Status.Code.OK), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeAlreadyExists() {
    Assertions.assertEquals(
        DispatcherResponse.Code.SKIP,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.ALREADY_EXISTS), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeResourceExhausted() {
    Assertions.assertEquals(
        DispatcherResponse.Code.RETRY,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.RESOURCE_EXHAUSTED), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeFailedPrecondition() {
    Assertions.assertEquals(
        DispatcherResponse.Code.DLQ,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.FAILED_PRECONDITION), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeUnknown() {
    Assertions.assertEquals(
        DispatcherResponse.Code.INVALID,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(Status.fromCode(Status.Code.UNKNOWN), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeUnimplemented() {
    Assertions.assertEquals(
        DispatcherResponse.Code.INVALID,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.UNIMPLEMENTED), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testReportPermissionDeniedIssue() throws ExecutionException, InterruptedException {
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(
            CompletableFuture.completedFuture(
                GrpcResponse.of(Status.fromCode(Status.Code.PERMISSION_DENIED), null, false)));
    Assertions.assertEquals(
        DispatcherResponse.Code.INVALID,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());
    Mockito.verify(pipelineStateManager, Mockito.times(1))
        .reportIssue(job, KafkaPipelineIssue.PERMISSION_DENIED.getPipelineHealthIssue());
  }

  @Test
  public void testReportInvalidResponseIssue() throws ExecutionException, InterruptedException {
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(
            CompletableFuture.completedFuture(
                GrpcResponse.of(Status.fromCode(Status.Code.UNAVAILABLE), null, false)));
    Assertions.assertEquals(
        DispatcherResponse.Code.INVALID,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());
    Mockito.verify(pipelineStateManager, Mockito.times(1))
        .reportIssue(job, KafkaPipelineIssue.INVALID_RESPONSE_RECEIVED.getPipelineHealthIssue());
  }

  @Test
  public void testReportHighMedianLatencyIssue() throws ExecutionException, InterruptedException {
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(
            CompletableFuture.completedFuture(
                GrpcResponse.of(Status.fromCode(Status.Code.OK), null, false)));
    Mockito.when(latencyTracker.getStats())
        .thenReturn(new LatencyTracker.Stats(10000, 100, 50, 2000, 1000));
    Assertions.assertEquals(
        DispatcherResponse.Code.COMMIT,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());
    Mockito.verify(pipelineStateManager, Mockito.times(1))
        .reportIssue(job, KafkaPipelineIssue.MEDIAN_RPC_LATENCY_HIGH.getPipelineHealthIssue());
  }

  @Test
  public void testReportHighMaxLatencyIssue() throws ExecutionException, InterruptedException {
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(
            CompletableFuture.completedFuture(
                GrpcResponse.of(Status.fromCode(Status.Code.OK), null, false)));
    Mockito.when(latencyTracker.getStats())
        .thenReturn(new LatencyTracker.Stats(10000, 100, 200, 2000, 1000));
    Assertions.assertEquals(
        DispatcherResponse.Code.COMMIT,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());
    Mockito.verify(pipelineStateManager, Mockito.times(1))
        .reportIssue(job, KafkaPipelineIssue.MAX_RPC_LATENCY_HIGH.getPipelineHealthIssue());
  }
}
