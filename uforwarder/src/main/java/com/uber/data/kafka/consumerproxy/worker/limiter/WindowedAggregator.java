package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.google.common.base.Ticker;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

/**
 * {@link WindowedAggregator} use ring buffer as time window buckets, and aggregate value over time
 * with time buckets
 *
 * <p>The implementation is not threadsafe, if you need threadsafe implementation considering wrap
 * the implementation with synchronization
 *
 * @param <T> the type parameter
 */
public class WindowedAggregator<T> {
  private final Ticker ticker;
  private final long bucketNano;
  private final Bucket<T>[] buckets;
  private final int nBuckets;
  private final BiFunction<T, T, T> aggregator;

  private WindowedAggregator(Builder builder, BiFunction<T, T, T> aggregator) {
    this.ticker = builder.ticker;
    this.bucketNano = builder.bucketNano;
    this.nBuckets = builder.nBuckets;
    this.buckets = new Bucket[nBuckets];
    for (int i = 0; i < nBuckets; ++i) {
      buckets[i] = new Bucket<>();
    }
    this.aggregator = aggregator;
  }

  /**
   * creates a builder
   *
   * @return the builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Puts a value into the aggregator
   *
   * @param t the t
   */
  public void put(T t) {
    long bucketId = ticker.read() / bucketNano;
    Bucket<T> bucket = buckets[(int) (bucketId % nBuckets)];
    if (bucket.bucketId == bucketId) {
      if (bucket.value == null) {
        bucket.value = t;
      } else {
        bucket.value = aggregator.apply(bucket.value, t);
      }
    } else {
      bucket.bucketId = bucketId;
      bucket.value = t;
    }
  }

  /**
   * Gets the aggregated value
   *
   * @param defaultValue the default value
   * @return the aggregated value, or default value if no value present
   */
  public T get(T defaultValue) {
    long bucketId = ticker.read() / bucketNano;
    T result = defaultValue;
    for (int i = 0; i < nBuckets; ++i) {
      int index = (int) ((bucketId - i + nBuckets) % nBuckets);
      Bucket<T> bucket = buckets[index];
      if (bucket.bucketId == bucketId - i) {
        result = aggregator.apply(result, bucket.value);
      }
    }
    return result;
  }

  public static class Builder {
    private Ticker ticker = Ticker.systemTicker();
    private int nBuckets = 10;
    private long bucketNano = Duration.ofSeconds(1).toNanos();

    /**
     * Sets ticker
     *
     * @param ticker the ticker
     * @return the builder
     */
    public Builder withTicker(Ticker ticker) {
      this.ticker = ticker;
      return this;
    }

    /**
     * Sets window duration and time unit
     *
     * @param duration the duration
     * @param unit the unit
     * @return the builder
     */
    public Builder withBucketDuration(long duration, TimeUnit unit) {
      if (duration <= 0) {
        throw new IllegalArgumentException("duration must be positive");
      }
      this.bucketNano = unit.toNanos(duration);
      return this;
    }

    /**
     * Sets number of buckets, higher the number is more accurate the aggregator is
     *
     * @param nBuckets the n buckets
     * @return the builder
     */
    public Builder withNBuckets(int nBuckets) {
      if (nBuckets <= 0) {
        throw new IllegalArgumentException("nBuckets must be positive");
      }
      this.nBuckets = nBuckets;
      return this;
    }

    /**
     * creates aggregator instance with specific aggregation function
     *
     * @param <T> the type parameter
     * @param aggregator the aggregator
     * @return the windowed aggregator
     */
    public <T> WindowedAggregator<T> of(BiFunction<T, T, T> aggregator) {
      if (aggregator == null) {
        throw new IllegalArgumentException("aggregator should not be null");
      }
      return new WindowedAggregator<>(this, aggregator);
    }
  }

  private class Bucket<T> {
    /** The Bucket id. */
    volatile long bucketId;
    /** The Value. */
    @Nullable volatile T value;
  }
}
