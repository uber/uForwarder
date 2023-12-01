package com.uber.data.kafka.instrumentation;

import java.util.function.BiConsumer;

/**
 * BiConsumerConverter is a utility class that convert checked BiConsumer to unchecked BiConsumer.
 */
public final class BiConsumerConverter {
  private BiConsumerConverter() {}

  public static <A, B, E extends Exception> BiConsumer<A, B> uncheck(
      ThrowingBiConsumer<A, B, E> checked) {
    return (a, b) -> {
      try {
        checked.accept(a, b);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }
}
