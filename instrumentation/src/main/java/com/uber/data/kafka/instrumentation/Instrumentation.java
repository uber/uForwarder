package com.uber.data.kafka.instrumentation;

import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.DurationBuckets;
import com.uber.m3.tally.Scope;
import com.uber.m3.util.Duration;
import io.grpc.ClientCall;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import net.logstash.logback.argument.StructuredArgument;
import net.logstash.logback.argument.StructuredArguments;
import org.slf4j.Logger;

/**
 * Instrumentation is an implementation of the Instrument interface that uses SL4J for logging,
 * Micrometer for metrics, and OpenTracing for tracing.
 *
 * <p>For logs, the following convention is provided:
 *
 * <ol>
 *   <li>Success: {name: "$name", "result": "success"}
 *   <li>Failure: {name: "$name", "result": "failure", "exception": "$stacktrace"}
 * </ol>
 *
 * For metrics, the following convention is provided:
 *
 * <ol>
 *   <li>Success: name:$name result:success
 *   <li>Failure: name:$name result:failure reason:exceptionSimpleName
 *   <li>Latency: name:$name.latency
 * </ol>
 *
 * Additionally, the provided varargs of tags are interpreted as "key, value, key, value, ..." and
 * added to the metrics as key-value pairs.
 *
 * <p>For tracing, a child span is created for the duration of the supplier caller.
 *
 * @implNote in order to remain lite, this interface assumes no additional dependencies such as
 *     ThreadLocalContext, lombok, or spring AOP. These can be added as syntactic sugar on top of
 *     this base implementation.
 */
public enum Instrumentation implements Instrument {
  instrument;

  // NOOP_CLOSEABLE that doesn't do anything. This is useful to avoid null-away checks.
  private static final Closeable NOOP_CLOSEABLE = new NoopClosable();
  private static final Buckets BUCKETS =
      new DurationBuckets(
          new Duration[] {
            Duration.ZERO,
            Duration.ofMillis(1),
            Duration.ofMillis(2),
            Duration.ofMillis(3),
            Duration.ofMillis(4),
            Duration.ofMillis(5),
            Duration.ofMillis(6),
            Duration.ofMillis(7),
            Duration.ofMillis(8),
            Duration.ofMillis(9),
            Duration.ofMillis(10),
            Duration.ofMillis(12),
            Duration.ofMillis(14),
            Duration.ofMillis(16),
            Duration.ofMillis(18),
            Duration.ofMillis(20),
            Duration.ofMillis(25),
            Duration.ofMillis(30),
            Duration.ofMillis(35),
            Duration.ofMillis(40),
            Duration.ofMillis(45),
            Duration.ofMillis(50),
            Duration.ofMillis(60),
            Duration.ofMillis(70),
            Duration.ofMillis(80),
            Duration.ofMillis(90),
            Duration.ofMillis(100),
            Duration.ofMillis(150),
            Duration.ofMillis(200),
            Duration.ofMillis(250),
            Duration.ofMillis(300),
            Duration.ofMillis(350),
            Duration.ofMillis(400),
            Duration.ofMillis(450),
            Duration.ofMillis(500),
            Duration.ofMillis(600),
            Duration.ofMillis(700),
            Duration.ofMillis(800),
            Duration.ofMillis(900),
            Duration.ofMillis(1000),
            Duration.ofMillis(1250),
            Duration.ofMillis(1500),
            Duration.ofMillis(1750),
            Duration.ofMillis(2000),
            Duration.ofMillis(2250),
            Duration.ofMillis(2500),
            Duration.ofMillis(3000),
            Duration.ofMillis(3500),
            Duration.ofMillis(4000),
            Duration.ofMillis(4500),
            Duration.ofMillis(5000),
            Duration.ofSeconds(6),
            Duration.ofSeconds(7),
            Duration.ofSeconds(8),
            Duration.ofSeconds(9),
            Duration.ofSeconds(10),
            Duration.ofSeconds(20),
            Duration.ofSeconds(30),
            Duration.ofSeconds(40),
            Duration.ofSeconds(50),
            Duration.ofSeconds(60)
          });

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
   */
  @Override
  public <T, E extends Exception> T withException(
      Logger logger,
      Scope scope,
      @Nullable Tracer tracer,
      ThrowingSupplier<T, E> supplier,
      Function<T, Boolean> resultChecker,
      String name,
      String... tags)
      throws E {
    @Nullable Span span = null;
    if (tracer != null) {
      Span parentSpan = tracer.activeSpan();
      span = startSpan(tracer, parentSpan, name);
    }
    long startNs = System.nanoTime();
    boolean isFailure = false;
    Closeable tracing = getTracingScope(tracer, span);
    try {
      T t = supplier.get();
      try {
        boolean success = resultChecker.apply(t);
        if (!success) {
          logAndMetricsFailure(logger, scope, System.nanoTime() - startNs, name, tags);
          isFailure = true;
        }
      } catch (Throwable ignored) {
        // treats exception as success, so we don't set isFailure.
      }
      return t;
    } catch (RuntimeException | Error e) {
      isFailure = true;
      logAndMetricsFailure(logger, scope, e, System.nanoTime() - startNs, name, tags);
      throw e;
    } catch (Exception ex) {
      isFailure = true;
      // supplier can only return type E for checked exception so we can safely cast to E.
      @SuppressWarnings("unchecked")
      E e = (E) ex;
      logAndMetricsFailure(logger, scope, e, System.nanoTime() - startNs, name, tags);
      throw e;
    } finally {
      if (!isFailure) {
        logAndMetricsSuccess(logger, scope, System.nanoTime() - startNs, name, tags);
      }
      // tracing implements Closeable interface which may throw IOException, but
      // the actual implementation of tracer scope never throws IOException so we can use safeClose.
      safeClose(tracing);
      if (span != null) {
        span.finish();
      }
    }
  }

  /**
   * WithExceptionalCompletion executes the provided supplier, propagating any exception completion.
   *
   * @param logger to invoke for writing logs.
   * @param scope to emit metrics.
   * @param tracer to use for distributed tracing.
   * @param supplier is the closure that should be executed.
   * @param name that is used to identify this call.
   * @param tags to add to logger/metrics/trace.
   * @param <T> the type to be returned.
   * @return CompletionStage that is returned by the supplier.
   */
  @Override
  public <T> CompletionStage<T> withExceptionalCompletion(
      Logger logger,
      Scope scope,
      @Nullable Tracer tracer,
      Supplier<CompletionStage<T>> supplier,
      String name,
      String... tags) {
    @Nullable Span span = null;
    if (tracer != null) {
      Span parentSpan = tracer.activeSpan();
      span = startSpan(tracer, parentSpan, name);
    }
    final Span immutableSpan = span;
    Closeable tracingScope = getTracingScope(tracer, span);
    long startNs = System.nanoTime();
    return supplier
        .get()
        .whenComplete(
            (r, t) -> {
              // tracerScope does not throw exception but we have to catch IOException b/c
              // compiler enforces this since they implement Closeable.
              safeClose(tracingScope);
              if (immutableSpan != null) {
                immutableSpan.finish();
              }
              if (t != null) {
                logAndMetricsFailure(logger, scope, t, System.nanoTime() - startNs, name, tags);
              } else {
                logAndMetricsSuccess(logger, scope, System.nanoTime() - startNs, name, tags);
              }
            });
  }

  /**
   * WithStreamObserver is an instrumented gRPC async call.
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
  @Override
  public <Req, Res> void withStreamObserver(
      Logger logger,
      Scope scope,
      @Nullable Tracer tracer,
      TriConsumer<ClientCall<Req, Res>, Req, StreamObserver<Res>> consumer,
      ClientCall<Req, Res> clientCall,
      Req request,
      StreamObserver<Res> responseStreamObserver,
      String name,
      String... tags) {
    @Nullable Span span = null;
    if (tracer != null) {
      Span parentSpan = tracer.activeSpan();
      span = startSpan(tracer, parentSpan, name);
    }
    final Closeable tracingScope = getTracingScope(tracer, span);
    final long startNs = System.nanoTime();
    InstrumentedStreamObserver instrumentedStreamObserver =
        new InstrumentedStreamObserver<>(
            logger, scope, tracingScope, span, responseStreamObserver, startNs, name, tags);
    try {
      consumer.accept(clientCall, request, instrumentedStreamObserver);
    } catch (Throwable t) {
      instrumentedStreamObserver.onError(t);
    }
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
    @Nullable Span span = null;
    if (tracer != null) {
      Span parentSpan = tracer.activeSpan();
      span = startSpan(tracer, parentSpan, name);
    }
    final Closeable tracingScope = getTracingScope(tracer, span);
    final long startNs = System.nanoTime();
    InstrumentedStreamObserver<Res> instrumentedStreamObserver =
        new InstrumentedStreamObserver<>(
            logger, scope, tracingScope, span, responseStreamObserver, startNs, name, tags);
    try {
      consumer.accept(request, instrumentedStreamObserver);
    } catch (Throwable t) {
      instrumentedStreamObserver.onError(t);
    }
  }

  /**
   * Starts a new child span.
   *
   * @param tracer to use to build span.
   * @param parent span to associate with this child span.
   * @param name of the child span.
   * @return child span or null if parent span was null.
   */
  @Nullable
  private Span startSpan(Tracer tracer, @Nullable Span parent, String name) {
    if (parent == null) {
      return null;
    }
    return tracer.buildSpan(name).asChildOf(parent).start();
  }

  /**
   * Gets the tracing scope as a closeable. Per Jaeger documentation, tracing scope must be closed
   * after the span is finished.
   *
   * @param tracer to register the span into.
   * @param span to register.
   * @return Closeable that should be closed to mark the end of a span.
   */
  private Closeable getTracingScope(@Nullable Tracer tracer, @Nullable Span span) {
    if (tracer == null || span == null) {
      return NOOP_CLOSEABLE;
    }
    return tracer.scopeManager().activate(span);
  }

  /**
   * LogAndMetricsFailure emits logs and metrics for failure.
   *
   * @param logger to write logs to.
   * @param durationNs is the duration of the call in nanoseconds for the latency measurement.
   * @param name of the call.
   * @param tags to add.
   */
  private void logAndMetricsFailure(
      Logger logger, Scope scope, long durationNs, String name, String... tags) {
    logger.error(name, loggingTags(Tags.Value.failure, tags));
    Scope taggedScope = scope.tagged(metricsTags(Tags.Value.failure, tags));
    reportCountAndLatency(taggedScope, name, durationNs);
  }

  /**
   * LogAndMetricsFailure emits logs and metrics for failure.
   *
   * @param logger to write logs to.
   * @param throwable that that was raised.
   * @param durationNs is the duration of the call in nanoseconds for the latency measurement.
   * @param name of the call.
   * @param tags to add.
   */
  private void logAndMetricsFailure(
      Logger logger,
      Scope scope,
      Throwable throwable,
      long durationNs,
      String name,
      String... tags) {
    logger.error(name, loggingTags(throwable, Tags.Value.failure, tags));
    Scope taggedScope = scope.tagged(metricsTags(throwable, Tags.Value.failure, tags));
    reportCountAndLatency(taggedScope, name, durationNs);
  }

  /**
   * logAndMetricsSuccess emits logs and metrics for success.
   *
   * @param logger to write logs to.
   * @param name of the call.
   * @param tags to add.
   */
  private void logAndMetricsSuccess(
      Logger logger, Scope scope, long durationNs, String name, String... tags) {
    logger.debug(name, loggingTags(Tags.Value.success, tags));
    Scope taggedScope = scope.tagged(metricsTags(Tags.Value.success, tags));
    reportCountAndLatency(taggedScope, name, durationNs);
  }

  private void reportCountAndLatency(Scope taggedScope, String name, long durationNs) {
    taggedScope.counter(name).inc(1);
    taggedScope.histogram(name + ".latency", BUCKETS).recordDuration(Duration.ofNanos(durationNs));
  }

  /**
   * SanitizeThrowable returns the simple class name for the throwable. This is useful when adding
   * error to metrics.
   *
   * @param throwable to sanitize.
   * @return simple classname for the throwable.
   */
  private String sanitizeThrowable(Throwable throwable) {
    return throwable.getClass().getSimpleName();
  }

  /**
   * Generate micrometer metric tags as key, value, key, value, ... string array.
   *
   * @param throwable to log with {@code Tags.Key.reason} tag.
   * @param resultValue to log with {@code Tags.Key.result} tag.
   * @param tags are the remaining tags to add.
   * @return metric tags as a map of key-value pairs.
   */
  private Map<String, String> metricsTags(Throwable throwable, String resultValue, String... tags) {
    Map<String, String> map = new HashMap<>();
    Utils.copyTags(map, tags);
    map.put(Tags.Key.result, resultValue);
    map.put(Tags.Key.reason, sanitizeThrowable(throwable));
    return map;
  }

  /**
   * Generate metric tags as key, value, key, value, ... string array.
   *
   * @param resultValue to log with {@code Tags.Key.result} tag.
   * @param code is a status code to add to metrics.
   * @param tags are the remaining tags to add.
   * @return metric tags as key, value, key, value, ... string array.
   */
  private static Map<String, String> metricsTags(String resultValue, String code, String... tags) {
    Map<String, String> map = new HashMap<>();
    Utils.copyTags(map, tags);
    map.put(Tags.Key.result, resultValue);
    map.put(Tags.Key.code, code);
    return map;
  }

  /**
   * Generate micrometer metric tags as key, value, key, value, ... string array.
   *
   * @param resultValue to log with {@code Tags.Key.result} tag.
   * @param tags are the remaining tags to add.
   * @return metric tags as key, value, key, value, ... string array.
   */
  private static Map<String, String> metricsTags(String resultValue, String... tags) {
    Map<String, String> map = new HashMap<>();
    Utils.copyTags(map, tags);
    map.put(Tags.Key.result, resultValue);
    return map;
  }

  /**
   * Generate an array of KeyValue object to add to SL4J logger for structured JSON logging.
   *
   * @param resultValue to log with {@code Tags.Key.result} tag.
   * @param tags are other tags to log.
   * @return Object[] that contains KeyValue objects to to provided to SLF4J logger.
   */
  private Object[] loggingTags(String resultValue, String... tags) {
    // allocate +1 size to array to append result tags.
    Object[] keyValues = new StructuredArgument[(tags.length / 2) + 1];
    int insertPosition = copyTags(keyValues, tags);

    Object[] additionalTags = {StructuredArguments.keyValue(Tags.Key.result, resultValue)};
    System.arraycopy(additionalTags, 0, keyValues, insertPosition, 1);
    return keyValues;
  }

  /**
   * Generate an array of KeyValue object to add to SL4J logger for structured JSON logging.
   *
   * @param resultValue to log with {@code Tags.Key.result} tag.
   * @param code to log with {@code Tags.Key.code} tag.
   * @param tags are other tags to log.
   * @return Object[] that contains KeyValue objects to to provided to SLF4J logger.
   */
  private static Object[] loggingTags(String resultValue, String code, String... tags) {
    // allocate +1 size to array to append result tags.
    Object[] keyValues = new StructuredArgument[(tags.length / 2) + 2];
    int insertPosition = copyTags(keyValues, tags);

    Object[] additionalTags = {
      StructuredArguments.keyValue(Tags.Key.result, resultValue),
      StructuredArguments.keyValue(Tags.Key.code, code)
    };
    System.arraycopy(additionalTags, 0, keyValues, insertPosition, 1);
    return keyValues;
  }

  /**
   * Generate an array of KeyValue object to add to SL4J logger for structured JSON logging.
   *
   * @param throwable to log with {@code Tags.Key.reason} tag.
   * @param resultValue to log with {@code Tags.Key.result} tag.
   * @param tags are other tags to log.
   * @return Object[] that contains KeyValue objects to to provided to SLF4J logger.
   */
  private static Object[] loggingTags(Throwable throwable, String resultValue, String... tags) {
    // allocate +2 size to array to append result tags and throwable.
    Object[] keyValues = new Object[(tags.length / 2) + 2];
    int insertPosition = copyTags(keyValues, tags);

    Object[] additionalTags = {
      StructuredArguments.keyValue(Tags.Key.result, resultValue), throwable
    };
    System.arraycopy(additionalTags, 0, keyValues, insertPosition, 2);
    return keyValues;
  }

  /**
   * CopyTags from source array to destination array.
   *
   * @param destination array that should be preallocated to the correct size.
   * @param source array to copy tags from.
   * @return offset in the destination array that has been copied up to.
   */
  private static int copyTags(Object[] destination, String... source) {
    int insertPosition = 0;
    String key = "unknown";
    for (int i = 0; i < source.length; i++) {
      String tag = source[i];
      if (i % 2 == 0) {
        key = tag;
      } else {
        destination[insertPosition++] = StructuredArguments.keyValue(key, tag);
      }
    }
    return insertPosition;
  }

  /**
   * safeClose closes a closable while swallowing IOException.
   *
   * <p>This should only be used for implementations where we are confident that closeable does not
   * throw IOException.
   *
   * <p>VisibleForTesting.
   */
  static void safeClose(Closeable closeable) {
    try {
      closeable.close();
    } catch (IOException e) {
      // swallow IOException.
    }
  }

  private static class InstrumentedStreamObserver<T> implements StreamObserver<T> {
    private final Logger logger;
    private final Scope scope;
    private final Closeable tracingScope;
    private final @Nullable Span span;
    private final StreamObserver<T> innerStreamObserver;
    private final long startNs;
    private final String name;
    private final String[] tags;

    InstrumentedStreamObserver(
        Logger logger,
        Scope scope,
        Closeable tracingScope,
        @Nullable Span span,
        StreamObserver<T> innerStreamObserver,
        long startNs,
        String name,
        String... tags) {
      this.logger = logger;
      this.scope = scope;
      this.tracingScope = tracingScope;
      this.span = span;
      this.innerStreamObserver = innerStreamObserver;
      this.startNs = startNs;
      this.name = name;
      this.tags = tags;
    }

    @Override
    public void onNext(T value) {
      innerStreamObserver.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
      Status.Code code = Status.fromThrowable(t).getCode();
      scope.tagged(metricsTags(Tags.Value.failure, code.name(), tags)).counter(name).inc(1);
      scope
          .tagged(metricsTags(Tags.Value.failure, code.name(), tags))
          .histogram(name + ".latency", BUCKETS)
          .recordDuration(Duration.ofNanos(System.nanoTime() - startNs));
      if (!(t instanceof StatusException || t instanceof StatusRuntimeException)) {
        // It will handle following errors with in-memory retry
        // 1. IllegalStateException if call already canceled
        // 2. NullPointerException for https://github.com/grpc/grpc-java/issues/9185
        // 3. Other unknown errors
        logger.error(name, loggingTags(t, code.name(), tags));
      } else {
        logger.debug(name, loggingTags(t, code.name(), tags));
      }

      safeClose(tracingScope);
      if (span != null) {
        span.finish();
      }
      innerStreamObserver.onError(t);
    }

    @Override
    public void onCompleted() {
      scope
          .tagged(metricsTags(Tags.Value.success, Status.Code.OK.name(), tags))
          .counter(name)
          .inc(1);
      scope
          .tagged(metricsTags(Tags.Value.success, Status.Code.OK.name(), tags))
          .histogram(name + ".latency", BUCKETS)
          .recordDuration(Duration.ofNanos(System.nanoTime() - startNs));
      logger.debug(name, loggingTags(Tags.Value.success, Status.Code.OK.name(), tags));
      safeClose(tracingScope);
      if (span != null) {
        span.finish();
      }
      innerStreamObserver.onCompleted();
    }
  }
}
