package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcResponse;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;
import io.grpc.Status;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import net.logstash.logback.argument.StructuredArgument;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class MessageStubTest extends FievelTestBase {

  @Test
  public void testCancelStub() {
    Map<DispatcherResponse.Code, AtomicInteger> codeCount = new HashMap<>();
    MessageStub stub = new MessageStub();
    MessageStub.Attempt attempt = stub.newAttempt();
    attempt.onCancel(
        () -> {
          codeCount
              .computeIfAbsent(stub.cancelCode().get(), c -> new AtomicInteger())
              .incrementAndGet();
        });
    boolean result = stub.cancel(DispatcherResponse.Code.DLQ);
    Assert.assertTrue(result);
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.ATTEMPT_CANCEL.toString()))
            .count());
    Assert.assertEquals(1, codeCount.get(DispatcherResponse.Code.DLQ).get());
    Assert.assertFalse(codeCount.containsKey(DispatcherResponse.Code.RETRY));
    result = stub.cancel(DispatcherResponse.Code.DLQ);
    Assert.assertFalse(result);
    Assert.assertEquals(DispatcherResponse.Code.DLQ, stub.cancelCode().get());
  }

  @Test
  public void testCancelledStub() {
    MessageStub stub = new MessageStub();
    Map<DispatcherResponse.Code, AtomicInteger> codeCount = new HashMap<>();
    MessageStub.Attempt attempt = stub.newAttempt();
    boolean result = stub.cancel(DispatcherResponse.Code.RETRY);
    Assert.assertTrue(result);
    attempt.onCancel(
        () -> {
          codeCount
              .computeIfAbsent(stub.cancelCode().get(), c -> new AtomicInteger())
              .incrementAndGet();
        });
    Assert.assertEquals(1, codeCount.get(DispatcherResponse.Code.RETRY).get());
    Assert.assertFalse(codeCount.containsKey(DispatcherResponse.Code.DLQ));
    result = stub.cancel(DispatcherResponse.Code.DLQ);
    Assert.assertEquals(DispatcherResponse.Code.RETRY, stub.cancelCode().get());
    Assert.assertFalse(result);
  }

  @Test
  public void testRetryAfterCancel() {
    MessageStub stub = new MessageStub();
    stub.cancel(DispatcherResponse.Code.RETRY);
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.PASSIVE_CANCEL.toString()))
            .count());
    CompletableFuture future = new CompletableFuture();
    // retry should not be canceled w/o any attempt
    stub.withRetryFuture(future);
    Assert.assertFalse(future.isCancelled());
  }

  @Test
  public void testNewAttemptAfterCancel() throws ExecutionException, InterruptedException {
    MessageStub stub = new MessageStub();
    stub.cancel(DispatcherResponse.Code.RETRY);
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.PASSIVE_CANCEL.toString()))
            .count());
    MessageStub.Attempt attempt = stub.newAttempt();
    GrpcResponse response =
        attempt
            .complete(CompletableFuture.completedFuture(GrpcResponse.of(Status.CANCELLED)))
            .toCompletableFuture()
            .get();
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.ATTEMPT_CANCELED.toString()))
            .count());
    Assert.assertTrue(stub.getCurrentAttempt() == null);
    Assert.assertEquals(DispatcherResponse.Code.RETRY, response.code().get());
    Assert.assertEquals(Status.CANCELLED.getCode(), response.status().getCode());
  }

  @Test
  public void testCancelAfterNewAttempt() throws ExecutionException, InterruptedException {
    MessageStub stub = new MessageStub();

    CompletableFuture<GrpcResponse> future = new CompletableFuture();
    MessageStub.Attempt attempt = stub.newAttempt();
    attempt.onCancel(() -> future.complete(GrpcResponse.of(Status.CANCELLED)));
    stub.cancel(DispatcherResponse.Code.RETRY);
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.ATTEMPT_CANCEL.toString()))
            .count());
    GrpcResponse response = attempt.complete(future).toCompletableFuture().get();
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.ATTEMPT_CANCELED.toString()))
            .count());
    Assert.assertTrue(stub.getCurrentAttempt() == null);
    Assert.assertEquals(DispatcherResponse.Code.RETRY, response.code().get());
    Assert.assertEquals(Status.CANCELLED.getCode(), response.status().getCode());
  }

  @Test
  public void testCloseAfterCommit() throws ExecutionException, InterruptedException {
    MessageStub stub = new MessageStub();
    MessageStub.Attempt attempt = stub.newAttempt();
    GrpcResponse response =
        attempt
            .complete(CompletableFuture.completedFuture(GrpcResponse.of(Status.OK)))
            .toCompletableFuture()
            .get();
    Assert.assertFalse(stub.cancel(DispatcherResponse.Code.RETRY));
    Assert.assertTrue(stub.getCurrentAttempt() == null);
    Assert.assertFalse(response.code().isPresent());
    Assert.assertEquals(Status.OK.getCode(), response.status().getCode());
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.CLOSED.toString()))
            .count());
  }

  @Test
  public void cancelAfterAcquireInfligtPermit() {
    MessageStub stub = new MessageStub();
    CompletableFuture future = new CompletableFuture();
    CompletableFuture stubFuture = stub.withFuturePermit(future);
    Assert.assertTrue(stub.cancel(DispatcherResponse.Code.RETRY));
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.PERMIT_CANCEL.toString()))
            .count());
    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(stubFuture.isCompletedExceptionally());
  }

  @Test
  public void acquireInfligtPermitAfterCancel() {
    MessageStub stub = new MessageStub();
    Assert.assertTrue(stub.cancel(DispatcherResponse.Code.RETRY));
    CompletableFuture future = new CompletableFuture();
    CompletableFuture stubFuture = stub.withFuturePermit(future);
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(
                event -> event.contains(MessageStub.DebugStatus.PERMIT_PASSIVE_CANCELED.toString()))
            .count());
    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(stubFuture.isCompletedExceptionally());
  }

  @Test
  public void testCancelRetryAfterNewAttempt() {
    MessageStub stub = new MessageStub();

    MessageStub.Attempt attempt = stub.newAttempt();
    attempt.complete(CompletableFuture.completedFuture(GrpcResponse.of(Status.CANCELLED)));
    CompletableFuture retryFuture = new CompletableFuture();
    // retry should not be canceled w/o any attempt
    stub.withRetryFuture(retryFuture);
    stub.cancel(DispatcherResponse.Code.RETRY);
    Assert.assertEquals(
        1,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.RETRY_CANCEL.toString()))
            .count());
    Assert.assertTrue(retryFuture.isCancelled());
  }

  @Test
  public void testCancelFailedWithException() {
    Scope scope = Mockito.mock(Scope.class);
    Counter counter = Mockito.mock(Counter.class);
    Mockito.when(scope.counter(Mockito.anyString())).thenReturn(counter);
    MessageStub stub = new MessageStub(new StructuredArgument[] {}, scope);
    MessageStub.Attempt attempt = stub.newAttempt();
    attempt.onCancel(
        () -> {
          throw new RuntimeException("test");
        });
    try {
      stub.cancel(DispatcherResponse.Code.RETRY);
    } catch (Exception e) {
      throw new AssertionError("should not fail", e);
    }
    Mockito.verify(scope, Mockito.times(1)).counter("dispatcher.cancel.failure");
    Mockito.verify(counter, Mockito.atLeast(1)).inc(1);
  }

  @Test
  public void testLogDebugStatus() {
    MessageStub stub = new MessageStub();
    stub.logDebugStatus(MessageStub.DebugStatus.ATTEMPT_CANCEL);
    for (int i = 0; i < 10; ++i) {
      stub.logDebugStatus(MessageStub.DebugStatus.CLOSED);
    }
    Assert.assertEquals(10, stub.getDebugInfo().size());
    Assert.assertEquals(
        10,
        stub.getDebugInfo().stream()
            .filter(event -> event.contains(MessageStub.DebugStatus.CLOSED.toString()))
            .count());
  }
}
