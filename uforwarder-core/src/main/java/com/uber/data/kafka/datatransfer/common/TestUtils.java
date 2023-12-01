package com.uber.data.kafka.datatransfer.common;

import com.google.common.base.Ticker;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public final class TestUtils {

  /** Generate a random port that can e used for unit test help reduce the chance of flaky tests. */
  public static int getRandomPort() {
    // Get random port to start zk testing server.
    // This is necessary b/c monorepo runs multiple tests in parallel so we cannot
    // use a single fixed port.
    return ThreadLocalRandom.current().nextInt(1024, 65535);
  }

  public static class TestTicker extends Ticker {
    AtomicLong currentNano = new AtomicLong();

    @Override
    public long read() {
      return currentNano.get();
    }

    public void add(Duration d) {
      currentNano.addAndGet(d.toNanos());
    }
  }
}
