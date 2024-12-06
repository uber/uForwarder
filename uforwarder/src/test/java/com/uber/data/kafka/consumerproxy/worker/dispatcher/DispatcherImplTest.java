package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcDispatcher;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcResponse;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.ResqConfig;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.dispatchers.kafka.KafkaDispatcher;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;
import io.grpc.Status;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class DispatcherImplTest extends FievelTestBase {
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

  @Before
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
    dispatcher =
        new DispatcherImpl(coreInfra, grpcDispatcher, dlqProducer, Optional.of(resqProducer));
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
    Assert.assertFalse(dispatcher.isRunning());
    dispatcher.start();
    Assert.assertTrue(dispatcher.isRunning());
    dispatcher.stop();
    Assert.assertFalse(dispatcher.isRunning());
    Mockito.verify(resqProducer, Mockito.times(1)).stop();
    // close one more time
    dispatcher.stop();
    Assert.assertFalse(dispatcher.isRunning());

    Mockito.doThrow(new RuntimeException()).when(grpcDispatcher).stop();
    Mockito.when(grpcDispatcher.isRunning()).thenReturn(true);
    RuntimeException exception = null;
    try {
      dispatcher.stop();
    } catch (RuntimeException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }

  @Test
  public void testSubmitGrpcMessage() throws Exception {
    // Test status code propagation
    // We have a separate unit test for the mapping from grpc status code to dispatcher response
    // code.
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(
            CompletableFuture.completedFuture(GrpcResponse.of(Status.fromCode(Status.Code.OK))));
    Assert.assertEquals(
        DispatcherResponse.Code.COMMIT,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());

    // Test exceptional completion is propagated up.
    CompletableFuture<GrpcResponse> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException());
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(future);
    Assert.assertTrue(
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
    Assert.assertEquals(
        DispatcherResponse.Code.DLQ,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());
  }

  @Test
  public void testSubmitKafkaMessage() throws Exception {
    // Test that null return on CompletableFuture<Void> is translated to COMMIT response code
    Mockito.when(dlqProducer.submit(ItemAndJob.of(kafkaDispatcherMessage.getProducerRecord(), job)))
        .thenReturn(CompletableFuture.completedFuture(null));
    Assert.assertEquals(
        DispatcherResponse.Code.COMMIT,
        dispatcher.submit(ItemAndJob.of(kafkaDispatcherMessage, job)).get().getCode());

    // Test exceptional completion is propagated up.
    CompletableFuture<DispatcherResponse> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException());
    Mockito.when(dlqProducer.submit(ItemAndJob.of(kafkaDispatcherMessage.getProducerRecord(), job)))
        .thenReturn(future);
    Assert.assertTrue(
        dispatcher.submit(ItemAndJob.of(kafkaDispatcherMessage, job)).isCompletedExceptionally());
  }

  @Test
  public void testSubmitResqKafkaMessage() throws ExecutionException, InterruptedException {
    Mockito.when(
            resqProducer.submit(ItemAndJob.of(kafkaResqDispatcherMessage.getProducerRecord(), job)))
        .thenReturn(CompletableFuture.completedFuture(null));
    Assert.assertEquals(
        DispatcherResponse.Code.COMMIT,
        dispatcher.submit(ItemAndJob.of(kafkaResqDispatcherMessage, job)).get().getCode());
  }

  @Test(expected = IllegalStateException.class)
  public void testSubmitResqKafkaMessageWithoutResqProducer() throws Throwable {
    DispatcherImpl badDispatcher =
        new DispatcherImpl(coreInfra, grpcDispatcher, dlqProducer, Optional.empty());
    try {
      badDispatcher.submit(ItemAndJob.of(kafkaResqDispatcherMessage, job)).get();
    } catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  @Test
  public void testSubmitGrpcMessageWithSkipResponseCode() throws Exception {
    Mockito.when(grpcDispatcher.submit(ItemAndJob.of(grpcDispatcherMessage.getGrpcMessage(), job)))
        .thenReturn(
            CompletableFuture.completedFuture(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.DEADLINE_EXCEEDED), DispatcherResponse.Code.SKIP)));
    Assert.assertEquals(
        DispatcherResponse.Code.SKIP,
        dispatcher.submit(ItemAndJob.of(grpcDispatcherMessage, job)).get().getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatusOverDue() {
    Assert.assertEquals(
        DispatcherResponse.Code.BACKOFF,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(Status.fromCode(Status.Code.UNAVAILABLE), Optional.empty(), true))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeOk() {
    Assert.assertEquals(
        DispatcherResponse.Code.COMMIT,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(Status.fromCode(Status.Code.OK), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeAlreadyExists() {
    Assert.assertEquals(
        DispatcherResponse.Code.SKIP,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.ALREADY_EXISTS), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeResourceExhausted() {
    Assert.assertEquals(
        DispatcherResponse.Code.RETRY,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.RESOURCE_EXHAUSTED), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeFailedPrecondition() {
    Assert.assertEquals(
        DispatcherResponse.Code.DLQ,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.FAILED_PRECONDITION), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeUnknown() {
    Assert.assertEquals(
        DispatcherResponse.Code.INVALID,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(Status.fromCode(Status.Code.UNKNOWN), Optional.empty(), false))
            .getCode());
  }

  @Test
  public void testDispatcherResponseFromGrpcStatus_CodeUnimplemented() {
    Assert.assertEquals(
        DispatcherResponse.Code.INVALID,
        DispatcherImpl.dispatcherResponseFromGrpcStatus(
                GrpcResponse.of(
                    Status.fromCode(Status.Code.UNIMPLEMENTED), Optional.empty(), false))
            .getCode());
  }
}
