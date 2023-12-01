package com.uber.data.kafka.datatransfer.worker.common;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.springframework.context.Lifecycle;

/**
 * Sink is a common interface that is used to represent a downstream step in the DataTransfer
 * pipeline.
 *
 * <p>A caller of the sink may enqueue objects from upstream (caller) to downstream (sink).
 */
public interface Sink<IN, RESPONSE> extends Lifecycle {
  /**
   * Submit an object to the sink (next stage in a processing pipeline).
   *
   * @param record to enqueue.
   * @return CompletionStage that completes when the sink has completed its action.
   */
  default CompletionStage<RESPONSE> submit(ItemAndJob<IN> record) {
    CompletableFuture<RESPONSE> future = new CompletableFuture<>();
    future.completeExceptionally(new IllegalStateException("unimplemented sink"));
    return future;
  }

  @Override
  default void start() {}

  @Override
  default void stop() {}

  @Override
  default boolean isRunning() {
    return true;
  }
}
