package com.uber.data.kafka.instrumentation;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.m3.tally.Scope;
import io.grpc.ClientCall;
import io.grpc.stub.StreamObserver;
import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstrumentTest {
  private static final Logger logger = LoggerFactory.getLogger(InstrumentTest.class);
  private Tracer tracer;
  private AtomicLong callCount;
  private Scope scope;
  private Instrument instrument;

  @BeforeEach
  public void setup() {
    callCount = new AtomicLong();
    scope = Mockito.mock(Scope.class);
    tracer = new MockTracer();
    instrument =
        new Instrument() {
          @Override
          public <T, E extends Exception> T withException(
              Logger logger,
              Scope scope,
              Tracer tracer,
              ThrowingSupplier<T, E> supplier,
              Function<T, Boolean> resultChecker,
              String name,
              String... tags)
              throws E {
            callCount.incrementAndGet();
            T t = supplier.get();
            Boolean ignored = resultChecker.apply(t);
            return t;
          }

          @Override
          public <T> CompletionStage<T> withExceptionalCompletion(
              Logger logger,
              Scope scope,
              Tracer tracer,
              Supplier<CompletionStage<T>> supplier,
              String name,
              String... tags) {
            callCount.incrementAndGet();
            return supplier.get();
          }

          @Override
          public <Req, Res> void withStreamObserver(
              Logger logger,
              Scope scope,
              Tracer tracer,
              TriConsumer<ClientCall<Req, Res>, Req, StreamObserver<Res>> consumer,
              ClientCall<Req, Res> clientCall,
              Req request,
              StreamObserver<Res> responseStreamObserver,
              String name,
              String... tags) {
            callCount.incrementAndGet();
          }

          @Override
          public <Req, Res> void withStreamObserver(
              Logger logger,
              Scope scope,
              Tracer tracer,
              BiConsumer<Req, StreamObserver<Res>> consumer,
              Req request,
              StreamObserver<Res> responseStreamObserver,
              String name,
              String... tags) {
            callCount.incrementAndGet();
          }
        };
  }

  @Test
  public void testWithExceptionWithResultChecker() throws Exception {
    instrument.withException(logger, scope, null, () -> true, r -> r, "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testWithExceptionWithoutTracer() throws Exception {
    instrument.withException(logger, scope, () -> true, "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testWithExceptionWithoutTracerWithResultChecker() throws Exception {
    instrument.withException(logger, scope, () -> true, r -> r, "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testReturnVoidWithException() throws Exception {
    instrument.returnVoidWithException(
        logger, scope, tracer, () -> {}, "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testReturnVoidCatchWithoutTracerThrowableSuccess() throws Exception {
    instrument.returnVoidCatchThrowable(logger, scope, () -> {}, "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testReturnVoidCatchThrowableWithoutTracerThrowsCheckedException() throws Exception {
    instrument.returnVoidCatchThrowable(
        logger,
        scope,
        () -> {
          throw new Exception();
        },
        "name",
        "tagKey",
        "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testReturnVoidCatchThrowableWithoutTracerThrowsUncheckedException() throws Exception {
    instrument.returnVoidCatchThrowable(
        logger,
        scope,
        () -> {
          throw new RuntimeException();
        },
        "name",
        "tagKey",
        "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testReturnVoidCatchThrowableSuccess() throws Exception {
    instrument.returnVoidCatchThrowable(
        logger, scope, tracer, () -> {}, "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testReturnVoidCatchThrowableThrowsCheckedException() throws Exception {
    instrument.returnVoidCatchThrowable(
        logger,
        scope,
        tracer,
        () -> {
          throw new Exception();
        },
        "name",
        "tagKey",
        "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testReturnVoidCatchThrowableThrowsUncheckedException() throws Exception {
    instrument.returnVoidCatchThrowable(
        logger,
        scope,
        tracer,
        () -> {
          throw new RuntimeException();
        },
        "name",
        "tagKey",
        "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testReturnVoidWithExceptionWithoutTracer() throws Exception {
    instrument.returnVoidWithException(logger, scope, () -> {}, "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testWithRuntimeException() {
    instrument.withRuntimeException(logger, scope, tracer, () -> 1, "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testWithRuntimeExceptionWithoutTracer() {
    instrument.withRuntimeException(logger, scope, () -> 1, "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void testWithRuntimeExceptionThrowsCheckedException() {
    assertThrows(
        RuntimeException.class,
        () -> {
          instrument.withRuntimeException(
              logger,
              scope,
              tracer,
              () -> {
                throw new IOException();
              },
              "name",
              "tagKey",
              "tagValue");
          Assertions.assertEquals(1, callCount.get());
        });
  }

  @Test
  public void testWithRuntimeExceptionThrowsRuntimeException() {
    assertThrows(
        RuntimeException.class,
        () -> {
          instrument.withRuntimeException(
              logger,
              scope,
              tracer,
              () -> {
                throw new RuntimeException();
              },
              "name",
              "tagKey",
              "tagValue");
          Assertions.assertEquals(1, callCount.get());
        });
  }

  @Test
  public void testWithRuntimeExceptionWithoutTracerThrowsRuntimeException() {
    assertThrows(
        RuntimeException.class,
        () -> {
          instrument.withRuntimeException(
              logger,
              scope,
              () -> {
                throw new RuntimeException();
              },
              "name",
              "tagKey",
              "tagValue");
          Assertions.assertEquals(1, callCount.get());
        });
  }

  @Test
  public void testWithRuntimeExceptionWithoutTracerThrowsCheckedException() {
    assertThrows(
        RuntimeException.class,
        () -> {
          instrument.withRuntimeException(
              logger,
              scope,
              () -> {
                throw new IOException();
              },
              "name",
              "tagKey",
              "tagValue");
          Assertions.assertEquals(1, callCount.get());
        });
  }

  @Test
  public void withExceptionalCompletion() {
    instrument.withExceptionalCompletion(
        logger, scope, () -> CompletableFuture.completedFuture(true), "name", "tagKey", "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }

  @Test
  public void withStreamObserverWithoutTracer() {
    instrument.withStreamObserver(
        logger,
        scope,
        (a, b, c) -> {},
        Mockito.mock(ClientCall.class),
        true,
        Mockito.mock(StreamObserver.class),
        "name",
        "tagKey",
        "tagValue");
    Assertions.assertEquals(1, callCount.get());
  }
}
