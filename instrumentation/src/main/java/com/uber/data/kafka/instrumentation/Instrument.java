package com.uber.data.kafka.instrumentation;

import com.uber.m3.tally.Scope;
import io.grpc.ClientCall;
import io.grpc.stub.StreamObserver;
import io.opentracing.Tracer;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.slf4j.Logger;

/**
 * Instrument is a common interface that standardizes success/failure/latency logging, metrics, and
 * tracing.
 *
 * @implNote in order to remain lite, this interface assumes no additional dependencies such as
 *     ThreadLocalContext, lombok, or spring AOP. These can be added as syntactic sugar on top of
 *     this base implementation.
 */
public interface Instrument {
  /**
   * WithException executes the provided supplier, executes the provided resultChecker to decide
   * whether the result of the provided supplier is success or failure, instruments based on the
   * result of the provided resultChecker and rethrows any checked exception.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param tracer to use for distributed tracing.
   * @param supplier is the closure that should be executed.
   * @param resultChecker is the function that is used to check whether the result of the supplier
   *     is success or failure. A returned value true means the result is success, a returned value
   *     false means the result is failure. We also treat exception as success. If the result is
   *     failure, it will be instrumented as failure.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <T> the type to be returned.
   * @param <E> the type of the checked exception.
   * @return the value returned by the supplier when invoked.
   * @throws E, which was thrown by the supplier.
   *     <p>Usage:
   *     <pre>
   *   T t = Instrumentation.instrument.withException(
   *     logger,
   *     scope,
   *     tracer,
   *     () -> methodToWrapThatReturnsT(),
   *     r -> methodTakesTAndReturnsBoolean(r),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  <T, E extends Exception> T withException(
      Logger logger,
      Scope scope,
      @Nullable Tracer tracer,
      ThrowingSupplier<T, E> supplier,
      Function<T, Boolean> resultChecker,
      String name,
      String... tags)
      throws E;

  /**
   * WithException executes the provided supplier and rethrows any checked exception.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param tracer to use for distributed tracing.
   * @param supplier is the closure that should be executed.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <T> the type to be returned.
   * @param <E> the type of the checked exception.
   * @return the value returned by the supplier when invoked.
   * @throws E, which was thrown by the supplier.
   *     <p>Usage:
   *     <pre>
   *   T t = Instrumentation.instrument.withException(
   *     logger,
   *     scope,
   *     tracer,
   *     () -> methodToWrapThatReturnsT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <T, E extends Exception> T withException(
      Logger logger,
      Scope scope,
      @Nullable Tracer tracer,
      ThrowingSupplier<T, E> supplier,
      String name,
      String... tags)
      throws E {
    return withException(logger, scope, tracer, supplier, r -> true, name, tags);
  }

  /**
   * WithExceptionalCompletion executes the provided supplier, propagating any exceptional
   * completion.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param supplier is the closure that should be executed.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <T> the type to be returned.
   * @return CompletionStage that is returned by the supplier.
   *     <p>Usage:
   *     <pre>
   *   CompletionStage<T> t = Instrumentation.instrument.withExceptionalCompletion(
   *     logger,
   *     scope,
   *     tracer,
   *     () -> asyncMethodToWrapThatReturnsCompletionStageT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <T> CompletionStage<T> withExceptionalCompletion(
      Logger logger,
      Scope scope,
      Supplier<CompletionStage<T>> supplier,
      String name,
      String... tags) {
    return withExceptionalCompletion(logger, scope, null, supplier, name, tags);
  }

  /**
   * WithExceptionalCompletion executes the provided supplier, propagating any exceptional
   * completion.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param tracer to use for distributed tracing.
   * @param supplier is the closure that should be executed.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <T> the type to be returned.
   * @return CompletionStage that is returned by the supplier.
   *     <p>Usage:
   *     <pre>
   *   CompletionStage<T> t = Instrumentation.instrument.withExceptionalCompletion(
   *     logger,
   *     scope,
   *     tracer,
   *     () -> asyncMethodToWrapThatReturnsCompletionStageT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  <T> CompletionStage<T> withExceptionalCompletion(
      Logger logger,
      Scope scope,
      @Nullable Tracer tracer,
      Supplier<CompletionStage<T>> supplier,
      String name,
      String... tags);

  /**
   * WithException executes the provided supplier and rethrows any checked exception.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param supplier is the closure that should be executed.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <T> the type to be returned.
   * @param <E> the type of the checked exception.
   * @return the value returned by the supplier when invoked.
   * @throws E, which was thrown by the supplier.
   *     <p>Usage:
   *     <pre>
   *   T t = Instrumentation.instrument.withException(
   *     logger,
   *     scope,
   *     () -> methodToWrapThatReturnsT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <T, E extends Exception> T withException(
      Logger logger, Scope scope, ThrowingSupplier<T, E> supplier, String name, String... tags)
      throws E {
    return withException(logger, scope, null, supplier, name, tags);
  }

  /**
   * WithException executes the provided supplier, executes the provided resultChecker to decide
   * whether the result of the provided supplier is success or failure, instruments based on the
   * result of the provided resultChecker and rethrows any checked exception.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param supplier is the closure that should be executed.
   * @param resultChecker is the function that is used to check whether the result of the supplier
   *     is success or failure. A returned value true means the result is success, a returned value
   *     false means the result is failure. We also treat exception as success. If the result is
   *     failure, it will be instrumented as failure.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <T> the type to be returned.
   * @param <E> the type of the checked exception.
   * @return the value returned by the supplier when invoked.
   * @throws E which was thrown by the supplier.
   *     <p>Usage:
   *     <pre>
   *   T t = Instrumentation.instrument.withException(
   *     logger,
   *     scope,
   *     () -> methodToWrapThatReturnsT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <T, E extends Exception> T withException(
      Logger logger,
      Scope scope,
      ThrowingSupplier<T, E> supplier,
      Function<T, Boolean> resultChecker,
      String name,
      String... tags)
      throws E {
    return withException(logger, scope, null, supplier, resultChecker, name, tags);
  }

  /**
   * returnVoidWithException executes the provided runnable and rethrows any checked exception.
   *
   * <p>This is syntactic sugar around {@code withException(..., Supplier<Void> supplier, ...)}.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param runnable is the closure that should be executed.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <E> the type of the checked exception.
   * @throws E, which was thrown by the runnable.
   *     <p>Usage:
   *     <pre>
   *   Instrumentation.instrument.returnVoidWithException(
   *     logger,
   *     scope,
   *     tracer,
   *     () -> methodToWrapThatReturnsT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <E extends Exception> void returnVoidWithException(
      Logger logger, Scope scope, ThrowingRunnable<E> runnable, String name, String... tags)
      throws E {
    withException(
        logger,
        scope,
        null,
        new ThrowingSupplier<Void, E>() {
          @Override
          public Void get() throws E {
            runnable.run();
            return null;
          }
        },
        name,
        tags);
  }

  /**
   * returnVoidWithException executes the provided runnable and rethrows any checked exception.
   *
   * <p>This is syntactic sugar around {@code withException(..., Supplier<Void> supplier, ...)}.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param tracer to use for distributed tracing.
   * @param runnable is the closure that should be executed.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <E> the type of the checked exception.
   * @throws E, which was thrown by the runnable.
   *     <p>Usage:
   *     <pre>
   *   Instrumentation.instrument.returnVoidWithException(
   *     logger,
   *     scope,
   *     () -> methodToWrapThatReturnsT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <E extends Exception> void returnVoidWithException(
      Logger logger,
      Scope scope,
      Tracer tracer,
      ThrowingRunnable<E> runnable,
      String name,
      String... tags)
      throws E {
    withException(
        logger,
        scope,
        tracer,
        new ThrowingSupplier<Void, E>() {
          @Override
          public Void get() throws E {
            runnable.run();
            return null;
          }
        },
        name,
        tags);
  }

  /**
   * returnVoidCatchThrowable executes the provided runnable and catches the checked exceptions and
   * runtime errors.
   *
   * <p>It is useful to wrap runnable within ScheduledExecutorService with this to avoid terminating
   * the scheduled work:
   * https://stackoverflow.com/questions/6894595/scheduledexecutorservice-exception-handling.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param runnable is the closure that should be executed.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <E> the type of the checked exception.
   * @throws E, which may thrown by the runnable, but is caught and swallowed.
   *     <p>Usage:
   *     <pre>
   *   Instrumentation.instrument.returnVoidCatchThrowable(
   *     logger,
   *     scope,
   *     () -> methodToWrapThatReturnsT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <E extends Exception> void returnVoidCatchThrowable(
      Logger logger, Scope scope, ThrowingRunnable<E> runnable, String name, String... tags) {
    // duplicate the implementation in returnVoidCatchThrowable with Tracer in order to avoid
    // exposing Nullable API to caller.
    try {
      withException(
          logger,
          scope,
          null,
          new ThrowingSupplier<Void, E>() {
            @Override
            public Void get() throws E {
              runnable.run();
              return null;
            }
          },
          name,
          tags);
    } catch (Throwable e) {
      // swallow exception
      // no need to log or metrics b/c that is handled in withException in a unified way.
    }
  }

  /**
   * returnVoidCatchThrowable executes the provided runnable and catches the checked exceptions and
   * runtime errors.
   *
   * <p>It is useful to wrap runnable within ScheduledExecutorService with this to avoid terminating
   * the scheduled work:
   * https://stackoverflow.com/questions/6894595/scheduledexecutorservice-exception-handling.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param tracer to use for distributed tracing.
   * @param runnable is the closure that should be executed.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <E> the type of the checked exception.
   * @throws E, which may thrown by the runnable, but is caught and swallowed.
   *     <p>Usage:
   *     <pre>
   *   Instrumentation.instrument.returnVoidCatchThrowable(
   *     logger,
   *     scope,
   *     () -> methodToWrapThatReturnsT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <E extends Exception> void returnVoidCatchThrowable(
      Logger logger,
      Scope scope,
      Tracer tracer,
      ThrowingRunnable<E> runnable,
      String name,
      String... tags) {
    try {
      withException(
          logger,
          scope,
          tracer,
          new ThrowingSupplier<Void, E>() {
            @Override
            public Void get() throws E {
              runnable.run();
              return null;
            }
          },
          name,
          tags);
    } catch (Throwable e) {
      // swallow exception
      // no need to log or metrics b/c that is handled in withException in a unified way.
    }
  }

  /**
   * WithRuntimeException executes the provided supplier.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param tracer to use for distributed tracing.
   * @param supplier is the closure that should be executed. Checked exceptions thrown by the
   *     supplier converted to RuntimeExceptions. converted to RuntimeExceptions. converted to
   *     RuntimeExceptions. converted to RuntimeExceptions.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <T> the type to be returned.
   * @return the value returned by the supplier when invoked.
   *     <p>Usage:
   *     <pre>
   *   T t = Instrumentation.instrument.withRuntimeException(
   *     logger,
   *     scope,
   *     tracer,
   *     () -> methodToWrapThatReturnsT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <T, E extends Exception> T withRuntimeException(
      Logger logger,
      Scope scope,
      Tracer tracer,
      ThrowingSupplier<T, E> supplier,
      String name,
      String... tags) {
    try {
      return withException(logger, scope, tracer, supplier::get, name, tags);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * WithRuntimeException executes the provided supplier.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param supplier is the closure that should be executed. Checked exceptions thrown by the
   *     supplier converted to RuntimeExceptions. converted to RuntimeExceptions. converted to
   *     RuntimeExceptions. converted to RuntimeExceptions.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <T> the type to be returned.
   * @return the value returned by the supplier when invoked.
   *     <p>Usage:
   *     <pre>
   *   T t = Instrumentation.instrument.withRuntimeException(
   *     logger,
   *     scope,
   *     () -> methodToWrapThatReturnsT(),
   *     "action-to-include-in-metrics-and-logs",
   *     "tag-key",
   *     "tag-value"
   *   );
   * </pre>
   */
  default <T, E extends Exception> T withRuntimeException(
      Logger logger, Scope scope, ThrowingSupplier<T, E> supplier, String name, String... tags) {
    try {
      return withException(logger, scope, null, supplier::get, name, tags);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * WithStreamObserver is an instrumented gRPC async client call.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param consumer that is invoked by passing through clientCall, request, and
   *     responseStreamObserver.
   * @param clientCall is the gRPC client call.
   * @param request to pass through.
   * @param responseStreamObserver is the callback that is invoked in response to gRPC request.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <Req> is the type of the request.
   * @param <Res> is the type of the response.
   */
  default <Req, Res> void withStreamObserver(
      Logger logger,
      Scope scope,
      TriConsumer<ClientCall<Req, Res>, Req, StreamObserver<Res>> consumer,
      ClientCall<Req, Res> clientCall,
      Req request,
      StreamObserver<Res> responseStreamObserver,
      String name,
      String... tags) {
    withStreamObserver(
        logger,
        scope,
        null, /* tracer */
        consumer,
        clientCall,
        request,
        responseStreamObserver,
        name,
        tags);
  }

  /**
   * WithStreamObserver is an instrumented gRPC async client call.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param tracer to use for distributed tracing.
   * @param consumer that is invoked by passing through clientCall, request, and
   *     responseStreamObserver.
   * @param clientCall is the gRPC client call.
   * @param request to pass through.
   * @param responseStreamObserver is the callback that is invoked in response to gRPC request.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <Req> is the type of the request.
   * @param <Res> is the type of the response.
   */
  <Req, Res> void withStreamObserver(
      Logger logger,
      Scope scope,
      @Nullable Tracer tracer,
      TriConsumer<ClientCall<Req, Res>, Req, StreamObserver<Res>> consumer,
      ClientCall<Req, Res> clientCall,
      Req request,
      StreamObserver<Res> responseStreamObserver,
      String name,
      String... tags);

  /**
   * WithStreamObserver is an instrumented gRPC async server call.
   *
   * @param logger to invoke for writing logs.
   * @param scope for emitting m3 metrics.
   * @param tracer to use for distributed tracing.
   * @param consumer that is invoked by with request and responseStreamObserver.
   * @param request to pass through.
   * @param responseStreamObserver is the callback that is invoked in response to gRPC request.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <Req> is the type of the request.
   * @param <Res> is the type of the response.
   */
  <Req, Res> void withStreamObserver(
      Logger logger,
      Scope scope,
      Tracer tracer,
      BiConsumer<Req, StreamObserver<Res>> consumer,
      Req request,
      StreamObserver<Res> responseStreamObserver,
      String name,
      String... tags);
}
