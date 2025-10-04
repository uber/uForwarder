package com.uber.data.kafka.consumerproxy.common;

import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * LingerSampler is a utility class that samples a value at most once per specified interval.
 *
 * @param <T> the type parameter
 */
public class LingerSampler<T> implements Supplier<T> {
  // default interval between samples 10 seconds
  private static final int DEFAULT_SAMPLE_LINGER_MS = 10000;
  private final Linger linger;
  private final Supplier<T> delegate;
  private final AtomicReference<T> lastSample;

  /**
   * Instantiates a new Linger sampler with default linger of 10 seconds.
   *
   * @param delegate the delegate
   */
  public LingerSampler(Supplier<T> delegate) {
    this(Ticker.systemTicker(), DEFAULT_SAMPLE_LINGER_MS, delegate);
  }

  /**
   * Instantiates a new Linger sampler.
   *
   * @param ticker the ticker
   * @param lingerMs the linger milliseconds between each sample
   * @param delegate the delegate
   */
  public LingerSampler(Ticker ticker, long lingerMs, Supplier<T> delegate) {
    this.linger = new Linger(ticker, TimeUnit.MILLISECONDS.toNanos(lingerMs));
    this.delegate = delegate;
    this.lastSample = new AtomicReference<>();
  }

  @Override
  public T get() {
    T ret = lastSample.get();
    if (ret == null || linger.tickIfNecessary()) {
      ret = delegate.get();
      lastSample.set(ret);
    }
    return ret;
  }

  /** The type Linger. */
  protected class Linger {
    private final long lingerNano;
    private final Ticker ticker;
    private AtomicLong lastTickNano = new AtomicLong(0);

    /**
     * Instantiates a new Linger.
     *
     * @param ticker the ticker
     * @param lingerNano the linger nano
     */
    Linger(Ticker ticker, long lingerNano) {
      this.ticker = ticker;
      this.lingerNano = lingerNano;
    }

    /**
     * Tick if interval is larger than the intervalNano, and update the last tick time
     *
     * @return true if interval is larger than the intervalNano
     */
    public boolean tickIfNecessary() {
      long now = ticker.read();
      long oldTick = lastTickNano.get();
      if (now - oldTick > lingerNano) {
        if (lastTickNano.compareAndSet(oldTick, now)) {
          return true;
        }
      }
      return false;
    }
  }
}
