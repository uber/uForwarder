package com.uber.data.kafka.datatransfer.common.context;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import net.jodah.failsafe.function.CheckedSupplier;

/**
 * ContextManager wraps functions and processors to serve context-aware functionality such as
 * tracking
 */
public interface ContextManager {
  ContextManager NOOP =
      new ContextManager() {

        @Override
        public <U> CompletableFuture<U> wrap(CompletableFuture<U> delegate) {
          return delegate;
        }

        @Override
        public ExecutorService wrap(ExecutorService delegate) {
          return delegate;
        }

        @Override
        public ScheduledExecutorService wrap(ScheduledExecutorService delegate) {
          return delegate;
        }

        @Override
        public void createRootContext() {}

        @Override
        public CompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
          return CompletableFuture.runAsync(runnable, executor);
        }

        @Override
        public <T> CheckedSupplier<T> wrap(CheckedSupplier<T> checkedSupplier) {
          return checkedSupplier;
        }
      };

  /**
   * Wraps the {@link CompletableFuture} into a context-aware {@link CompletableFuture}. The
   * context-aware {@link CompletableFuture} atuomatically wraps the submitted {@link Supplier},
   * {@link BiConsumer}, {@link BiFunction}, {@link Consumer}, {@link Function}, or {@link Runnable}
   * tasks into context-aware equivalents.
   *
   * @param delegate the {@link CompletableFuture}
   * @param <U> the {@link CompletableFuture} result type
   * @return context-propagating {@link CompletableFuture}
   */
  <U> CompletableFuture<U> wrap(CompletableFuture<U> delegate);

  /**
   * Wraps the {@link ExecutorService} with a context-propagating proxy. The context-aware {@link
   * ExecutorService} automatically creates context-aware tasks from submitted {@link Runnable} or
   * {@link Callable} tasks to propagate context from the task-creation thread to the task-execution
   * thread.
   *
   * @param delegate the {@link ExecutorService}
   * @return context-propagating {@link ExecutorService}
   */
  ExecutorService wrap(ExecutorService delegate);

  /**
   * Wraps the {@link ScheduledExecutorService} with a context-propagating proxy. The context-aware
   * {@link ScheduledExecutorService} automatically creates context-aware tasks from submitted
   * {@link Runnable} or {@link Callable} tasks to propagate context from the task-creation thread
   * to the task-execution thread.
   *
   * @param delegate the {@link ExecutorService}
   * @return context-propagating {@link ExecutorService}
   */
  ScheduledExecutorService wrap(ScheduledExecutorService delegate);

  /** Destroys current context and create a new context as root context */
  void createRootContext();

  /**
   * Converts the {@link Runnable} task into a context aware task before calling {@link
   * CompletableFuture#runAsync}. The context aware task propagates context from the task-creation
   * thread to the task-execution thread.
   *
   * @param runnable the {@link Runnable}
   * @return context-propagating {@link CompletableFuture}
   * @see CompletableFuture#runAsync(Runnable, Executor)
   */
  CompletableFuture<Void> runAsync(Runnable runnable, Executor executor);

  /**
   * Wraps the {@link CheckedSupplier} with a context-propagating proxy. The context-aware {@link
   * CheckedSupplier}* propagates current context from the task-creation thread to the
   * task-execution thread.
   *
   * @param checkedSupplier the {@link CheckedSupplier}
   * @return context-propagating {@link CheckedSupplier}
   */
  <T> CheckedSupplier<T> wrap(CheckedSupplier<T> checkedSupplier);
}
