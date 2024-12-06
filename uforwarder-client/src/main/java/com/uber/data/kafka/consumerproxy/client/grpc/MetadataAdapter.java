package com.uber.data.kafka.consumerproxy.client.grpc;

import com.google.common.collect.ImmutableList;
import io.grpc.Metadata;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * MetadataAdapter provides accessor methods for retrieving Kafka metadata from Grpc metadata
 *
 * <p>Kafka Metadata is stored as the following gRPC headers:
 *
 * <ul>
 *   <li>{@code kafka-topic} contains topic name
 *   <li>{@code kafka-consumergroup} contains consumer group name
 *   <li>{@code kafka-partition} contains partition id
 *   <li>{@code kafka-offset} contains offset value
 *   <li>{@code kafka-retrycount} contains retry count
 *   <li>{@code kafka-tracing-info} contains retry/dlq topic name,partition id,offset value. Purely
 *       for tracing purpose.
 * </ul>
 */
public final class MetadataAdapter {
  private static final Metadata.Key<String> TOPIC = asciiMetadataKey("kafka-topic");
  private static final Metadata.Key<String> CONSUMER_GROUP =
      asciiMetadataKey("kafka-consumergroup");
  private static final Metadata.Key<String> PARTITION = asciiMetadataKey("kafka-partition");
  private static final Metadata.Key<String> OFFSET = asciiMetadataKey("kafka-offset");
  private static final Metadata.Key<String> RETRY_COUNT = asciiMetadataKey("kafka-retrycount");
  private static final Metadata.Key<String> TRACING_INFO = asciiMetadataKey("kafka-tracing-info");
  private static final Metadata.Key<String> CONSUMER_RECORD_TIMESTAMP_HEADER_KEY =
      asciiMetadataKey("kafka-record-ts");

  // Headers with the following prefix is now allowed for retrieval.
  private static final List<String> DISALLOWED_HEADER_PREFIXES =
      ImmutableList.of(
          "kafka" // Kafka reserved headers, must use a specific method
          );
  private final Supplier<Metadata> metadataSupplier;

  /**
   * Creates new MetadataManager
   *
   * @param metadataSupplier
   */
  public MetadataAdapter(Supplier<Metadata> metadataSupplier) {
    this.metadataSupplier = metadataSupplier;
  }

  /**
   * @return the Kafka topic name associated with this request or empty string if one was not found.
   */
  public String getTopic() {
    return readString(TOPIC);
  }

  /**
   * @return the Kafka consumer group associated with this request or empty string if one was not
   *     found.
   */
  public String getConsumerGroup() {
    return readString(CONSUMER_GROUP);
  }

  /** @return the Kafka partition associated with this request or -1 if one was not found. */
  public int getPartition() {
    return readInt(PARTITION);
  }

  /** @return the Kafka offset associated with this request or -1 if one was not found. */
  public long getOffset() {
    return readLong(OFFSET);
  }

  /** @return the Kafka retry count associated with this request or -1 if one was not found. */
  public long getRetryCount() {
    return readLong(RETRY_COUNT);
  }

  /** @return the Kafka consumer record timestamp, also know as produce timestamp for this record */
  public long getConsumerRecordTimestamp() {
    return readLong(CONSUMER_RECORD_TIMESTAMP_HEADER_KEY);
  }

  /**
   * Gets the header value with the supplied header key(case-irrelevant).
   *
   * @param headerKey the key for identifying the header
   * @return the string representing the header value
   */
  public String getHeader(String headerKey) {
    for (String prefix : DISALLOWED_HEADER_PREFIXES) {
      if (headerKey.toLowerCase(Locale.ROOT).startsWith(prefix)) {
        throw new IllegalArgumentException(
            String.format("disallowed header key with prefix `%s` supplied", prefix));
      }
    }
    return readString(asciiMetadataKey(headerKey));
  }

  /**
   * This field should be used only for tracing information and no business logic should be
   * implemented based on this.
   *
   * @return the Retry/DLQ Kafka topic name associated with this request or empty string if the
   *     message is not consumed from retry/dlq queue.
   */
  public String getTracingInfo() {
    return readString(TRACING_INFO);
  }

  private static Metadata.Key<String> asciiMetadataKey(String name) {
    return Metadata.Key.of(name, Metadata.ASCII_STRING_MARSHALLER);
  }

  private String readString(Metadata.Key<String> key) {
    Metadata metadata = metadataSupplier.get();
    try {
      String value = metadata.get(key);
      return value != null ? value : "";

    } catch (Throwable e) {
      // fallthrough to default return.
    }
    return "";
  }

  private int readInt(Metadata.Key<String> key) {
    Metadata metadata = metadataSupplier.get();
    try {
      String value = metadata.get(key);
      return Integer.parseInt(value);

    } catch (Throwable e) {
      // fallthrough to default return.
    }
    return -1;
  }

  private long readLong(Metadata.Key<String> key) {
    Metadata metadata = metadataSupplier.get();
    try {
      String value = metadata.get(key);
      return Long.parseLong(value);
    } catch (Throwable e) {
      // fallthrough to default return.
    }
    return -1;
  }
}
