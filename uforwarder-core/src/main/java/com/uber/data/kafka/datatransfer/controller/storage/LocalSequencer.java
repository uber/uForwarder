package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.api.core.InternalApi;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LocalSequencer is an implementation of Sequencer for testing which uses AtomicLong to distribute
 * a incrementing long in a threadsafe manner.
 *
 * <p>It will not provide consistent sequencing across nodes.
 */
@InternalApi
public class LocalSequencer<V> implements IdProvider<Long, V> {
  private static final long START = 1;
  private final AtomicLong atomicLong;

  public LocalSequencer() {
    this(START);
  }

  private LocalSequencer(long start) {
    atomicLong = new AtomicLong(start);
  }

  @Override
  public Long getId(V item) throws Exception {
    return atomicLong.getAndIncrement();
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public boolean isRunning() {
    return true;
  }
}
