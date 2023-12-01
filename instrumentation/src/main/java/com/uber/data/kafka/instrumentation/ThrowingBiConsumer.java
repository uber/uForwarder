package com.uber.data.kafka.instrumentation;

/**
 * ThrowingBiConsumer is an alternative to the standard Java {@code BiConsumer} that throws an
 * checked exception.
 */
@FunctionalInterface
public interface ThrowingBiConsumer<A, B, E extends Exception> {
  void accept(A a, B b) throws E;
}
