package com.uber.data.kafka.instrumentation;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * DirectSupplier is analogous to Guava Direct Executor.
 *
 * <p>It executes the provided supplier method on the thread that called it, converting any
 * exception into an exceptional completion of the future.
 */
public final class DirectSupplier {
  private DirectSupplier() {}

  public static <T> CompletionStage<T> supply(ThrowingSupplier<T, Exception> supplier) {
    CompletableFuture<T> future = new CompletableFuture<>();
    try {
      future.complete(supplier.get());
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }
}
