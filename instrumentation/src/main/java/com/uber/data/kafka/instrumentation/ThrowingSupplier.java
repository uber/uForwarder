package com.uber.data.kafka.instrumentation;

/**
 * ThrowingSupplier is an alternative to the standard Java {@code Supplier} that throws an checked
 * exception.
 *
 * @param <T> type to return.
 * @param <E> checked exception that may be thrown.
 */
@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
  T get() throws E;
}
