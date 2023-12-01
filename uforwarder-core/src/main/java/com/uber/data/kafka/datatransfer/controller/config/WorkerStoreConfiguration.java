package com.uber.data.kafka.datatransfer.controller.config;

import com.google.common.base.Preconditions;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for worker store.
 *
 * <p>This inherits from StoreConfiguration, which is wired up using spring Configuration.
 *
 * <p>Overrides are expressed in yaml as:
 *
 * <pre>
 *   # Base store config
 *   datatransfer:
 *     master:
 *       store:
 *         # base config goes here
 *         mode: ZK
 *         ttl: 0 # base TTL is zero (b/c job store and worker store don't need TTL)
 *       worker:
 *         # worker override config goes here
 *         ttl: 1m # worker TTL is set to 1m.
 * </pre>
 *
 * @implNote To allow overrides for each store type, we
 *     <ul>
 *       <li>Implement WorkerStoreConfigurationOverride that reads from the override config prefix.
 *       <li>Implement override merge in the getter.
 *     </ul>
 */
public class WorkerStoreConfiguration extends StoreConfiguration {
  private WorkerStoreConfigurationOverride workerStoreConfigurationOverride =
      new WorkerStoreConfigurationOverride();

  @Override
  public String getZkDataPath() {
    return "/workers/{id}";
  }

  @Override
  public String getZkSequencerPath() {
    return "/sequencer/worker";
  }

  @Override
  public Duration getBufferedWriteInterval() {
    return workerStoreConfigurationOverride.getBufferedWriteInterval();
  }

  @Override
  public Duration getTtl() {
    Duration ttl = super.getTtl();
    if (!workerStoreConfigurationOverride.ttl.isZero()) {
      ttl = workerStoreConfigurationOverride.getTtl();
    }
    // defensively assert that worker ttl must never be zero.
    Preconditions.checkArgument(!ttl.isZero(), "Worker Store TTL must not be zero");
    return ttl;
  }

  @Autowired
  public void setWorkerStoreConfigurationOverride(
      WorkerStoreConfigurationOverride workerStoreConfigurationOverride) {
    this.workerStoreConfigurationOverride = workerStoreConfigurationOverride;
  }

  // WorkerStore may want to set a ttl that overrides the default, so we define the override
  // fields here.
  @Configuration
  @ConfigurationProperties(prefix = "master.store.worker")
  public static class WorkerStoreConfigurationOverride {
    private Duration ttl = Duration.ZERO;
    private Duration bufferedWriteInterval = Duration.ofSeconds(60);

    public Duration getTtl() {
      return ttl;
    }

    public void setTtl(Duration ttl) {
      this.ttl = ttl;
    }

    public Duration getBufferedWriteInterval() {
      return bufferedWriteInterval;
    }

    public void setBufferedWriteInterval(Duration bufferedWriteInterval) {
      this.bufferedWriteInterval = bufferedWriteInterval;
    }
  }
}
