package com.uber.data.kafka.datatransfer.controller.config;

import com.uber.data.kafka.datatransfer.controller.storage.Mode;
import com.uber.data.kafka.datatransfer.controller.storage.SerializerType;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** General store configurations. */
@ConfigurationProperties(prefix = "master.store")
public class StoreConfiguration {

  // Backend for the store.
  // Default to ZK so that we don't accidentally make a mistake in production.
  // For development, override to Local if desired.
  private Mode mode = Mode.ZK;

  // Serializer to be used while storing / retrieving objects from ZK
  // Default to JSON so that KCP doesn't break
  // NOTE: The change is incompatible i.e if there are already existing objects in ZK with JSON and
  // if we change the type to PROTO, then  it would lead to SerDe failures
  private SerializerType serializerType = SerializerType.JSON;

  // ttl for the items.
  private Duration ttl = Duration.ZERO;

  // the interval for buffer write to permanent storage
  // ZERO means BufferedWriteDecorator is disabled -- put call skips in-memory cache and writes
  // directly to permanent storage.
  // A value large than ZERO means BufferedWriteDecorator is enabled, put call
  // writes to in-memory storage, which is periodically persisted to permanent storage.
  private Duration bufferedWriteInterval = Duration.ZERO;

  // Enables LoggingAndMetricsStoreDecorator
  private Boolean logDecoratorEnabled = true;

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public SerializerType getSerializerType() {
    return serializerType;
  }

  public void setSerializerType(SerializerType serializerType) {
    this.serializerType = serializerType;
  }

  public String getZkDataPath() {
    throw new IllegalStateException("base store configuration has no zk info");
  }

  public String getZkSequencerPath() {
    throw new IllegalStateException("base store configuration has no zk info");
  }

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

  public Boolean isLogDecoratorEnabled() {
    return logDecoratorEnabled;
  }

  public void setLogDecoratorEnabled(Boolean logDecoratorEnabled) {
    this.logDecoratorEnabled = logDecoratorEnabled;
  }
}
