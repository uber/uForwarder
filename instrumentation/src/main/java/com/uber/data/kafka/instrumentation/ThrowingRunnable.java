package com.uber.data.kafka.instrumentation;

/**
 * ThrowingRunnable is an alternative to the standard Java {@code Runnable} that throws an checked
 * exception.
 *
 * @param <E> checked exception that may be thrown.
 */
@FunctionalInterface
public interface ThrowingRunnable<E extends Exception> {
  void run() throws E;
}
