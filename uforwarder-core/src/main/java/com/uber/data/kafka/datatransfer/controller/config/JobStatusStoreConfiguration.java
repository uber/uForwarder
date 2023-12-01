package com.uber.data.kafka.datatransfer.controller.config;

import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Configuration for JobStatusStore. */
public class JobStatusStoreConfiguration extends StoreConfiguration {
  private JobStatusStoreConfigurationOverride jobStatusStoreConfigurationOverride =
      new JobStatusStoreConfigurationOverride();

  @Override
  public String getZkDataPath() {
    return "/jobstatus/{id}";
  }

  @Override
  public String getZkSequencerPath() {
    return "/sequencer/jobstatus";
  }

  @Override
  public Duration getBufferedWriteInterval() {
    return jobStatusStoreConfigurationOverride.getBufferedWriteInterval();
  }

  @Override
  public Duration getTtl() {
    Duration ttl = super.getTtl();
    if (!jobStatusStoreConfigurationOverride.ttl.isZero()) {
      ttl = jobStatusStoreConfigurationOverride.getTtl();
    }
    return ttl;
  }

  @Autowired
  public void setJobStatusStoreConfigurationOverride(
      JobStatusStoreConfigurationOverride jobStatusStoreConfigurationOverride) {
    this.jobStatusStoreConfigurationOverride = jobStatusStoreConfigurationOverride;
  }

  // JobStatusStoreConfiguration must override the default writeCachePersistInterval.
  @Configuration
  @ConfigurationProperties(prefix = "master.store.jobstatus")
  public static class JobStatusStoreConfigurationOverride {

    private Duration ttl = Duration.ZERO;
    private Duration bufferedWriteInterval = Duration.ofSeconds(10);

    public Duration getBufferedWriteInterval() {
      return bufferedWriteInterval;
    }

    public void setBufferedWriteInterval(Duration bufferedWriteInterval) {
      this.bufferedWriteInterval = bufferedWriteInterval;
    }

    public Duration getTtl() {
      return ttl;
    }

    public void setTtl(Duration ttl) {
      this.ttl = ttl;
    }
  }
}
