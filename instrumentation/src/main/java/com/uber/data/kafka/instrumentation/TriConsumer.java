package com.uber.data.kafka.instrumentation;

@FunctionalInterface
public interface TriConsumer<A, B, C> {
  void accept(A a, B b, C c);
}
