package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.uber.data.kafka.datatransfer.ScaleStoreSnapshot;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * In-memory store for auto-scalar job group status information.
 *
 * <p>This class provides a cache-based storage mechanism for auto-scalar status information
 * associated with job groups. It uses Google Guava's Cache implementation to provide automatic
 * expiration of entries based on access time, ensuring that stale status information is
 * automatically cleaned up.
 *
 * <p>The store maintains a mapping between {@link JobGroupKey} and {@link
 * AutoScalar.JobGroupScaleStatus} objects, allowing efficient storage and retrieval of scaling
 * status information for different job groups.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Automatic entry expiration based on access time
 *   <li>Configurable time-to-live (TTL) for entries
 *   <li>Thread-safe cache operations
 *   <li>Manual cleanup capabilities
 *   <li>Configurable ticker for time-based operations
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * AutoScalarConfiguration config = new AutoScalarConfiguration();
 * Ticker ticker = Ticker.systemTicker();
 * ScaleStatusStore store = new ScaleStatusStore(config, ticker);
 *
 * // Access the underlying map
 * Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> statusMap = store.asMap();
 *
 * // Clean up expired entries
 * store.cleanUp();
 * }</pre>
 *
 * <p>This class is thread-safe and can be used in multi-threaded environments.
 *
 * @see JobGroupKey
 * @see AutoScalar.JobGroupScaleStatus
 * @see AutoScalarConfiguration
 * @since 1.0
 */
@ThreadSafe
public class ScaleStatusStore {

  /**
   * In-memory cache for storing auto-scalar job group status information.
   *
   * <p>This cache automatically expires entries based on access time and uses the configured ticker
   * for time-based operations. The cache is thread-safe and provides efficient storage and
   * retrieval of scaling status information.
   */
  private final Cache<JobGroupKey, AutoScalar.JobGroupScaleStatus> store;

  private final Ticker ticker;

  /**
   * Constructs a new ScaleStatusStore with the specified configuration and ticker.
   *
   * <p>This constructor creates a cache-based store with automatic expiration based on the job
   * status TTL configuration. The cache will automatically remove entries that haven't been
   * accessed within the configured time period.
   *
   * @param config the auto-scalar configuration containing TTL settings
   * @param ticker the ticker to use for time-based operations
   */
  public ScaleStatusStore(AutoScalarConfiguration config, Ticker ticker) {
    store =
        CacheBuilder.newBuilder()
            .expireAfterAccess(config.getJobStatusTTL().toMillis(), TimeUnit.MILLISECONDS)
            .ticker(ticker)
            .build();
    this.ticker = ticker;
  }

  /**
   * Constructs a new ScaleStatusStore with the specified configuration using the system ticker.
   *
   * <p>This constructor creates a cache-based store using the system's default ticker for time
   * measurements. It's equivalent to calling the two-parameter constructor with {@link
   * Ticker#systemTicker()}.
   *
   * @param autoScalarConfiguration the auto-scalar configuration containing TTL settings
   */
  public ScaleStatusStore(AutoScalarConfiguration autoScalarConfiguration) {
    this(autoScalarConfiguration, Ticker.systemTicker());
  }

  /**
   * Returns the underlying cache as a map.
   *
   * <p>This method provides access to the current state of the cache as a standard Java Map. The
   * returned map reflects the current contents of the cache, including any entries that may be
   * expired but not yet cleaned up.
   *
   * <p>The map support modification as any normal map do
   *
   * @return a map of the current cache contents
   */
  public Map<JobGroupKey, AutoScalar.JobGroupScaleStatus> asMap() {
    return store.asMap();
  }

  /**
   * Performs cleanup of expired entries in the cache.
   *
   * <p>This method triggers the cache to remove any entries that have expired based on the
   * configured TTL. Cleanup is typically performed automatically by the cache, but this method
   * allows for manual cleanup when needed.
   *
   * <p>Manual cleanup can be useful in scenarios where immediate removal of expired entries is
   * required, such as during shutdown or maintenance operations.
   */
  public void cleanUp() {
    store.cleanUp();
  }

  public ScaleStoreSnapshot snapshot() {
    return ScaleStoreSnapshot.newBuilder()
        .addAllJobGroupSnapshot(
            asMap().values().stream()
                .map(AutoScalar.JobGroupScaleStatus::snapshot)
                .collect(Collectors.toList()))
        .setTimestampNanos(ticker.read())
        .build();
  }
}
