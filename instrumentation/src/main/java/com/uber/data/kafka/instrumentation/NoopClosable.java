package com.uber.data.kafka.instrumentation;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.Immutable;

/**
 * NoopCloseable is an implementation of closeable that doesn't do anything.
 *
 * <p>This is helpful to avoid null-away checks without adding {@code @Nullable} annotation
 * everywhere.
 */
@Immutable
final class NoopClosable implements Closeable {
  @Override
  public void close() throws IOException {}
}
