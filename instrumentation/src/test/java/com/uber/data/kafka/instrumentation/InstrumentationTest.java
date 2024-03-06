package com.uber.data.kafka.instrumentation;

import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;
import io.grpc.ClientCall;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.annotation.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstrumentationTest extends FievelTestBase {
  private static final Logger logger = LoggerFactory.getLogger(InstrumentationTest.class);
  private Scope scope;
  private Counter counter;
  private Timer timer;
  private Tracer tracer;
  private Histogram histogram;

  @Before
  public void setup() {
    scope = Mockito.mock(Scope.class);
    counter = Mockito.mock(Counter.class);
    timer = Mockito.mock(Timer.class);
    histogram = Mockito.mock(Histogram.class);
    Mockito.when(scope.tagged(Mockito.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(Mockito.anyString())).thenReturn(counter);
    Mockito.when(scope.timer(Mockito.anyString())).thenReturn(timer);
    Mockito.when(scope.histogram(Mockito.anyString(), Mockito.any())).thenReturn(histogram);
    tracer = new MockTracer();
  }

  @Test
  public void testWithExceptionWithResultCheckerReturnsSuccess() {
    String name = "testWithExceptionWithResultCheckerReturnsSuccess";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    boolean expected = true;
    boolean got =
        Instrumentation.instrument.withException(
            logger, scope, tracer, () -> expected, r -> true, name, "key", "value");
    Assert.assertEquals(expected, got);
    assertMetricsWithResultChecker(name, true);
  }

  @Test
  public void testWithExceptionWithResultCheckerReturnsFailure() {
    String name = "testWithExceptionWithResultCheckerReturnsFailure";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    boolean expected = true;
    boolean got =
        Instrumentation.instrument.withException(
            logger, scope, tracer, () -> expected, r -> false, name, "key", "value");
    Assert.assertEquals(expected, got);
    assertMetricsWithResultChecker(name, false);
  }

  @Test
  public void testWithExceptionWithResultCheckerThrows() {
    String name = "testWithExceptionWithResultCheckerThrows";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    boolean expected = true;
    boolean got =
        Instrumentation.instrument.withException(
            logger,
            scope,
            tracer,
            () -> expected,
            r -> {
              throw new RuntimeException();
            },
            name,
            "key",
            "value");
    Assert.assertEquals(expected, got);
    assertMetricsWithResultChecker(name, null);
  }

  @Test
  public void testWithExceptionWithoutTracerWithResultCheckerReturnsSuccess() {
    String name = "testWithExceptionWithoutTracerWithResultCheckerReturnsSuccess";
    boolean expected = true;
    boolean got =
        Instrumentation.instrument.withException(
            logger, scope, null, () -> expected, r -> true, name, "key", "value");
    Assert.assertEquals(expected, got);
    assertMetricsWithResultChecker(name, true);
  }

  @Test
  public void testWithExceptionWithoutTracerWithResultCheckerReturnsFailure() {
    String name = "testWithExceptionWithoutTracerWithResultCheckerReturnsFailure";
    boolean expected = true;
    boolean got =
        Instrumentation.instrument.withException(
            logger, scope, null, () -> expected, r -> false, name, "key", "value");
    Assert.assertEquals(expected, got);
    assertMetricsWithResultChecker(name, false);
  }

  @Test
  public void testWithExceptionWithoutTracerWithResultCheckerThrows() {
    String name = "testWithExceptionWithoutTracerWithResultCheckerThrows";
    boolean expected = true;
    boolean got =
        Instrumentation.instrument.withException(
            logger,
            scope,
            null,
            () -> expected,
            r -> {
              throw new RuntimeException();
            },
            name,
            "key",
            "value");
    Assert.assertEquals(expected, got);
    assertMetricsWithResultChecker(name, null);
  }

  private void assertMetricsWithResultChecker(String name, @Nullable Boolean checkResult) {
    Map<String, String> tags = new HashMap<>();
    tags.put("key", "value");
    if (checkResult == null || Boolean.TRUE.equals(checkResult)) {
      tags.put(Tags.Key.result, Tags.Value.success);
    } else { // Boolean.FALSE.equals(checkResult)
      tags.put(Tags.Key.result, Tags.Value.failure);
    }
    Mockito.verify(scope).tagged(tags);
    Mockito.verify(scope).counter(name);
    Mockito.verify(counter).inc(1);
    Mockito.verify(scope).histogram(Mockito.eq(name + ".latency"), Mockito.any());
  }

  @Test
  public void testWithExceptionSuccess() throws Exception {
    String name = "testWithExceptionSuccess";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    boolean expected = true;
    boolean got =
        Instrumentation.instrument.withException(
            logger, scope, tracer, () -> expected, name, "key", "value");
    Assert.assertEquals(expected, got);
    assertMetrics(name, null);
  }

  @Test
  public void testWithExceptionWithoutTracer() throws Exception {
    String name = "testWithExceptionSuccessWithoutTracer";
    boolean expected = true;
    boolean got =
        Instrumentation.instrument.withException(
            logger, scope, null, () -> expected, name, "key", "value");
    Assert.assertEquals(expected, got);
    assertMetrics(name, null);
  }

  @Test
  public void testWithExceptionCheckedFailure() throws Exception {
    String name = "testWithExceptionFailure";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    IOException expected = new IOException();
    boolean exceptionThrown = false;
    try {
      Instrumentation.instrument.withException(
          logger,
          scope,
          tracer,
          () -> {
            throw expected;
          },
          name,
          "key",
          "value");
    } catch (IOException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    assertMetrics(name, expected);
  }

  @Test
  public void testWithExceptionRuntimeFailure() throws Exception {
    String name = "testWithExceptionFailure";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    RuntimeException expected = new RuntimeException();
    boolean exceptionThrown = false;
    try {
      Instrumentation.instrument.withException(
          logger,
          scope,
          tracer,
          () -> {
            throw expected;
          },
          name,
          "key",
          "value");
    } catch (RuntimeException e) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    assertMetrics(name, expected);
  }

  @Test
  public void testWithExceptionalCompletionSuccess() throws Exception {
    String name = "testWithExceptionalCompletionSuccess";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    boolean expected = true;
    boolean got =
        Instrumentation.instrument
            .withExceptionalCompletion(
                logger,
                scope,
                tracer,
                () -> CompletableFuture.completedFuture(true),
                name,
                "key",
                "value")
            .toCompletableFuture()
            .get();
    Assert.assertEquals(expected, got);
    assertMetrics(name, null);
  }

  @Test
  public void testWithExceptionalCompletionWithoutTracer() throws Exception {
    String name = "testWithExceptionalCompletionWithoutTracer";
    boolean expected = true;
    boolean got =
        Instrumentation.instrument
            .withExceptionalCompletion(
                logger,
                scope,
                null,
                () -> CompletableFuture.completedFuture(true),
                name,
                "key",
                "value")
            .toCompletableFuture()
            .get();
    Assert.assertEquals(expected, got);
    assertMetrics(name, null);
  }

  @Test
  public void testWithExceptionalCompletionFailure() throws Exception {
    String name = "testWithExceptionalCompletionFailure";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    RuntimeException expected = new RuntimeException();
    CompletionStage<Boolean> got =
        Instrumentation.instrument.withExceptionalCompletion(
            logger,
            scope,
            tracer,
            () -> {
              CompletableFuture<Boolean> completableFuture = new CompletableFuture<>();
              completableFuture.completeExceptionally(expected);
              return completableFuture;
            },
            name,
            "key",
            "value");
    Assert.assertTrue(got.toCompletableFuture().isCompletedExceptionally());
    assertMetrics(name, expected);
  }

  @Test
  public void testSafeClose() throws Exception {
    Closeable closeable = Mockito.mock(Closeable.class);
    Instrumentation.safeClose(closeable);
    Mockito.verify(closeable, Mockito.times(1)).close();
  }

  @Test
  public void testSafeCloseSwallowsException() throws Exception {
    Closeable closeable = Mockito.mock(Closeable.class);
    Mockito.doThrow(new IOException()).when(closeable).close();
    Instrumentation.safeClose(closeable);
    Mockito.verify(closeable, Mockito.times(1)).close();
  }

  @Test
  public void testClientWithStreamObserverSuccess() throws Exception {
    String name = "testClientWithStreamObserverSuccess";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    ClientCall<Boolean, Boolean> clientCall = Mockito.mock(ClientCall.class);
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (c, r, s) -> {
          s.onNext(true);
          s.onCompleted();
        },
        clientCall,
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(1)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(0)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(1)).onCompleted();
  }

  @Test
  public void testWithClientStreamObserverSuccessWithoutTracer() throws Exception {
    String name = "testClientWithStreamObserverSuccessWithoutTracer";
    ClientCall<Boolean, Boolean> clientCall = Mockito.mock(ClientCall.class);
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (c, r, s) -> {
          s.onNext(true);
          s.onCompleted();
        },
        clientCall,
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(1)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(0)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(1)).onCompleted();
  }

  @Test
  public void testClientWithStreamObserverFailure() throws Exception {
    String name = "testClientWithStreamObserverFailure";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    ClientCall<Boolean, Boolean> clientCall = Mockito.mock(ClientCall.class);
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (c, r, s) -> {
          s.onError(new RuntimeException());
        },
        clientCall,
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(0)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(0)).onCompleted();
  }

  @Test
  public void testClientExceptionWithStreamObserverFailure() throws Exception {
    ClientCall<Boolean, Boolean> clientCall = Mockito.mock(ClientCall.class);
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (c, r, s) -> {
          throw new NullPointerException();
        },
        clientCall,
        true,
        streamObserver,
        "mock",
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(1))
        .onError(Mockito.any(NullPointerException.class));
  }

  @Test
  public void testClientWithStreamObserverFailureWithoutTracer() throws Exception {
    String name = "testClientWithStreamObserverFailureWithoutTracer";
    ClientCall<Boolean, Boolean> clientCall = Mockito.mock(ClientCall.class);
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (c, r, s) -> {
          s.onError(new RuntimeException());
        },
        clientCall,
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(0)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(0)).onCompleted();
  }

  private void assertMetrics(String name, @Nullable Throwable t) {
    Map<String, String> tags = new HashMap<>();
    tags.put("key", "value");
    if (t == null) {
      tags.put(Tags.Key.result, Tags.Value.success);
    } else {
      tags.put(Tags.Key.result, Tags.Value.failure);
      tags.put(Tags.Key.reason, t.getClass().getSimpleName());
    }

    Mockito.verify(scope).tagged(tags);
    Mockito.verify(scope).counter(name);
    Mockito.verify(counter).inc(1);
    Mockito.verify(scope).histogram(Mockito.matches(name + ".latency"), Mockito.any());
  }

  @Test
  public void testServerWithStreamObserverSuccess() throws Exception {
    String name = "testServerWithStreamObserverSuccess";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (r, s) -> {
          s.onNext(true);
          s.onCompleted();
        },
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(1)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(0)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(1)).onCompleted();
  }

  @Test
  public void testServerWithStreamObserverSuccessWithoutTracer() throws Exception {
    String name = "testServerWithStreamObserverSuccessWithoutTracer";
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (r, s) -> {
          s.onNext(true);
          s.onCompleted();
        },
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(1)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(0)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(1)).onCompleted();
  }

  @Test
  public void testServerWithStreamObserverFailure() throws Exception {
    String name = "testServerWithStreamObserverFailure";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (r, s) -> {
          s.onError(new RuntimeException());
        },
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(0)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(0)).onCompleted();
  }

  @Test
  public void testServerWithStreamObserverException() throws Exception {
    String name = "testServerWithStreamObserverException";
    Span span = tracer.buildSpan(name).start();
    tracer.activateSpan(span);
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (r, s) -> {
          throw new RuntimeException();
        },
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(0)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(0)).onCompleted();
  }

  @Test
  public void testServerWithStreamObserverFailureWithoutTracer() throws Exception {
    String name = "testServerWithStreamObserverFailureWithoutTracer";
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (r, s) -> {
          s.onError(new RuntimeException());
        },
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(0)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(0)).onCompleted();
  }

  @Test
  public void testServerWithStreamObserverErrorStatusWithoutTracer() throws Exception {
    String name = "testServerWithStreamObserverErrorStatusWithoutTracer";
    StreamObserver<Boolean> streamObserver = Mockito.mock(StreamObserver.class);
    Instrumentation.instrument.withStreamObserver(
        logger,
        scope,
        tracer,
        (r, s) -> {
          s.onError(Status.RESOURCE_EXHAUSTED.asRuntimeException());
        },
        true,
        streamObserver,
        name,
        "key",
        "value");
    Mockito.verify(streamObserver, Mockito.times(0)).onNext(Mockito.anyBoolean());
    Mockito.verify(streamObserver, Mockito.times(1)).onError(Mockito.any());
    Mockito.verify(streamObserver, Mockito.times(0)).onCompleted();
  }
}
