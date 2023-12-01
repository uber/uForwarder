package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.api.core.InternalApi;
import com.google.protobuf.Message;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.instrumentation.Instrumentation;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.slf4j.Logger;

/**
 * LoggingAndMetricsStoreDecorator wraps a concrete Store implementation with standardized metrics
 * and logging and tracing.
 */
@InternalApi
public final class LoggingAndMetricsStoreDecorator<K, V extends Message> implements Store<K, V> {
  private V prototype;
  private final Store<K, V> impl;
  private final Logger logger;
  private final CoreInfra infra;

  private LoggingAndMetricsStoreDecorator(
      V prototype, Logger logger, CoreInfra infra, Store<K, V> impl) {
    this.prototype = prototype;
    this.impl = impl;
    this.logger = logger;
    this.infra = infra;
  }

  public static <K, V extends Message> Store<K, V> decorate(
      V prototype, Logger logger, CoreInfra infra, Store<K, V> impl) {
    return new LoggingAndMetricsStoreDecorator<>(prototype, logger, infra, impl);
  }

  @Override
  public void start() {
    impl.start();
  }

  @Override
  public void stop() {
    impl.stop();
  }

  @Override
  public boolean isRunning() {
    return impl.isRunning();
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> initialized() throws Exception {
    return impl.initialized();
  }

  @Override
  public Map<K, Versioned<V>> getAll() throws Exception {
    return Instrumentation.instrument.withException(
        logger, infra.scope(), infra.tracer(), () -> impl.getAll(), getName("getall"));
  }

  @Override
  public Map<K, Versioned<V>> getAll(Function<V, Boolean> selector) throws Exception {
    return Instrumentation.instrument.withException(
        logger, infra.scope(), infra.tracer(), () -> impl.getAll(selector), getName("getall"));
  }

  @Override
  public Versioned<V> create(V item, BiFunction<K, V, V> creator) throws Exception {
    return Instrumentation.instrument.withException(
        logger, infra.scope(), infra.tracer(), () -> impl.create(item, creator), getName("create"));
  }

  @Override
  public Versioned<V> get(K id) throws Exception {
    return Instrumentation.instrument.withException(
        logger, infra.scope(), infra.tracer(), () -> impl.get(id), getName("get"));
  }

  @Override
  public Versioned<V> getThrough(K id) throws Exception {
    return Instrumentation.instrument.withException(
        logger, infra.scope(), infra.tracer(), () -> impl.getThrough(id), getName("getthrough"));
  }

  @Override
  public void put(K id, Versioned<V> item) throws Exception {
    Instrumentation.instrument.returnVoidWithException(
        logger, infra.scope(), infra.tracer(), () -> impl.put(id, item), getName("put"));
  }

  @Override
  public CompletionStage<Void> putAsync(K id, Versioned<V> item) {
    return Instrumentation.instrument.withExceptionalCompletion(
        logger, infra.scope(), infra.tracer(), () -> impl.putAsync(id, item), getName("putasync"));
  }

  @Override
  public void putThrough(K id, Versioned<V> item) throws Exception {
    Instrumentation.instrument.returnVoidWithException(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> impl.putThrough(id, item),
        getName("putthrough"));
  }

  @Override
  public void remove(K id) throws Exception {
    Instrumentation.instrument.returnVoidWithException(
        logger, infra.scope(), infra.tracer(), () -> impl.remove(id), getName("remove"));
  }

  private String getName(String suffix) {
    return prototype.getDescriptorForType().getName() + "." + suffix;
  }
}
