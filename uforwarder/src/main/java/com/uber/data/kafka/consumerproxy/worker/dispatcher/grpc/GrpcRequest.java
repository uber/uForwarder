package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import static com.uber.data.kafka.datatransfer.common.MetadataUtils.metadataInterceptor;

import com.google.protobuf.ByteString;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import io.grpc.ClientInterceptor;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.header.Headers;

public final class GrpcRequest {
  private static final String TOPIC_HEADER_KEY = "kafka-topic";
  private static final String CONSUMER_GROUP_HEADER_KEY = "kafka-consumergroup";
  private static final String PARTITION_HEADER_KEY = "kafka-partition";
  private static final String TRACING_INFO_KEY = "kafka-tracing-info";

  private static final String OFFSET_HEADER_KEY = "kafka-offset";
  private static final String RETRY_COUNT_HEADER_KEY = "kafka-retrycount";
  private static final String KAFKA_HEADER_KEY = "kafka-key-bin";
  private static final String ATTEMPT_COUNT_HEADER_KEY = "kafka-attemptcount";
  private static final String PRODUCE_TIMESTAMP_HEADER_KEY = "kafka-record-ts";

  private final String consumergroup;
  private final String topic;
  private final int partition;
  private final long offset;
  private final long retryCount;
  private final long dispatchAttempt;
  private final long timeoutCount;
  private final byte[] key;
  private final byte[] value;
  private final String physicalTopic;
  private final String physicalCluster;
  private final String tracingInfo;
  private final Headers headers;
  private final MessageStub stub;
  private final CompletableFuture<GrpcResponse> future;
  private final long consumerRecordTimestamp;

  public GrpcRequest(
      String consumergroup,
      String topic,
      int partition,
      long offset,
      MessageStub stub,
      long retryCount,
      long dispatchAttempt,
      long timeoutCount,
      byte[] value,
      String physicalTopic,
      String physicalCluster,
      int physicalPartition,
      long physicalOffset,
      Headers headers,
      long consumerRecordTimestamp) {
    this(
        consumergroup,
        topic,
        partition,
        offset,
        stub,
        retryCount,
        dispatchAttempt,
        timeoutCount,
        physicalTopic,
        physicalCluster,
        physicalPartition,
        physicalOffset,
        headers,
        value,
        new byte[] {},
        consumerRecordTimestamp);
  }

  public GrpcRequest(
      String consumergroup,
      String topic,
      int partition,
      long offset,
      MessageStub stub,
      long retryCount,
      long dispatchAttempt,
      long timeoutCount,
      String physicalTopic,
      String physicalCluster,
      int physicalPartition,
      long physicalOffset,
      Headers headers,
      byte[] value,
      byte[] key,
      long consumerRecordTimestamp) {
    this.consumergroup = consumergroup;
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.retryCount = retryCount;
    this.timeoutCount = timeoutCount;
    this.dispatchAttempt = dispatchAttempt;
    this.value = value;
    this.key = key;
    this.physicalTopic = physicalTopic;
    this.physicalCluster = physicalCluster;
    this.tracingInfo =
        new StringBuffer()
            .append("physical-topic=")
            .append(physicalTopic)
            .append(",")
            .append("physical-partition=")
            .append(physicalPartition)
            .append(",")
            .append("physical-offset=")
            .append(physicalOffset)
            .toString();
    this.headers = headers;
    this.stub = stub;
    this.future = new CompletableFuture<>();
    this.consumerRecordTimestamp = consumerRecordTimestamp;
  }

  ClientInterceptor[] metadataInterceptors() {

    ClientInterceptor[] fixedMetadataInterceptors =
        new ClientInterceptor[] {
          metadataInterceptor(CONSUMER_GROUP_HEADER_KEY, consumergroup),
          metadataInterceptor(TOPIC_HEADER_KEY, topic),
          metadataInterceptor(PARTITION_HEADER_KEY, Integer.toString(partition)),
          metadataInterceptor(OFFSET_HEADER_KEY, Long.toString(offset)),
          metadataInterceptor(RETRY_COUNT_HEADER_KEY, Long.toString(retryCount)),
          metadataInterceptor(KAFKA_HEADER_KEY, key),
          metadataInterceptor(ATTEMPT_COUNT_HEADER_KEY, Long.toString(dispatchAttempt)),
          metadataInterceptor(PRODUCE_TIMESTAMP_HEADER_KEY, Long.toString(consumerRecordTimestamp)),
        };
    return getClientInterceptors(fixedMetadataInterceptors);
  }

  private ClientInterceptor[] getClientInterceptors(ClientInterceptor[] fixedMetadataInterceptors) {
    // topic, partition, offset always corresponds to logical naming. This means even
    // in case of retry/dlq topic, the values of these 3 fields corresponds to origin topic.
    // Thus for retry/dlq send the physicalTopic, physicalPartition & physicalOffset
    // corresponds to **__retry / **__dlq topic name.
    //
    // Send the additional headers only if the physical Topic name is different from logical topic.
    // This is non-parseable fields for clients and clients should not rely on this information.
    // This will help reduce the number of keys sent over gRPC request.
    ClientInterceptor[] metadataInterceptors = fixedMetadataInterceptors;
    if (!this.physicalTopic.equals(this.topic)) {
      metadataInterceptors =
          ArrayUtils.add(
              fixedMetadataInterceptors, metadataInterceptor(TRACING_INFO_KEY, tracingInfo));
    }
    return metadataInterceptors;
  }

  ByteString payload() {
    if (value == null) {
      return ByteString.copyFrom("".getBytes());
    }
    return ByteString.copyFrom(value);
  }

  public String getConsumergroup() {
    return consumergroup;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }

  public long getRetryCount() {
    return retryCount;
  }

  public long getDispatchAttempt() {
    return dispatchAttempt;
  }

  public long getTimeoutCount() {
    return timeoutCount;
  }

  public Headers getHeaders() {
    return headers;
  }

  public MessageStub getStub() {
    return stub;
  }

  public String getPhysicalCluster() {
    return physicalCluster;
  }

  /**
   * Gets the produce timestamp of the consumer record.
   *
   * @return The timestamp of the consumer record.
   */
  public long getConsumerRecordTimestamp() {
    return consumerRecordTimestamp;
  }

  public CompletableFuture<GrpcResponse> getFuture() {
    return future;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GrpcRequest that = (GrpcRequest) o;
    return partition == that.partition
        && offset == that.offset
        && stub == that.stub
        && retryCount == that.retryCount
        && dispatchAttempt == that.dispatchAttempt
        && timeoutCount == that.timeoutCount
        && consumergroup.equals(that.consumergroup)
        && topic.equals(that.topic)
        && Arrays.equals(key, that.key)
        && Arrays.equals(value, that.value)
        && physicalTopic.equals(that.physicalTopic)
        && physicalCluster.equals(that.physicalCluster)
        && headers.equals(that.headers);
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            consumergroup,
            topic,
            partition,
            offset,
            stub,
            retryCount,
            dispatchAttempt,
            timeoutCount,
            physicalTopic,
            physicalCluster,
            headers);
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }
}
