package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcRequest;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

/**
 * DispatcherMessage wraps the message sent to {@code DispatcherImpl}, the message can be either a
 * {@code ProducerRecord} or a {@code KafkaMessage}
 */
public class DispatcherMessage {
  // This marks the type of the message that the dispatcher should handle.
  private final Type type;

  // This is the destination for the data.
  // If Type == KAFKA, this will contain the Kafka topic.
  // If Type == GRPC, this will contain the destination muttley URI.
  private final String destination;

  // The data to transmit.
  private final byte[] key;
  private final byte[] value;
  private final Headers headers;

  // Consumer Metadata for the topic.
  private final String group;
  private final String topic;
  private final int partition;
  private final long offset;

  private final String physicalTopic;
  private final String physicalCluster;
  private final int physicalPartition;
  private final long physicalOffset;

  private final long retryCount;
  private final long dispatchAttempt;
  private final long timeoutCount;
  private final MessageStub stub;

  public DispatcherMessage(
      Type type,
      String destination,
      byte[] key,
      byte[] value,
      Headers headers,
      String group,
      String topic,
      int partition,
      long offset,
      MessageStub stub,
      String physicalTopic,
      String physicalCluster,
      int physicalPartition,
      long physicalOffset,
      long retryCount,
      long dispatchAttempt,
      long timeoutCount) {
    this.type = type;
    this.destination = destination;
    this.key = key;
    this.value = value;
    this.headers = headers;
    this.group = group;
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.stub = stub;
    this.physicalTopic = physicalTopic;
    this.physicalCluster = physicalCluster;
    this.physicalPartition = physicalPartition;
    this.physicalOffset = physicalOffset;
    this.retryCount = retryCount;
    this.dispatchAttempt = dispatchAttempt;
    this.timeoutCount = timeoutCount;
  }

  public ProducerRecord<byte[], byte[]> getProducerRecord() {
    if (type != Type.KAFKA) {
      throw new IllegalStateException("this is not a Kafka producer record");
    }
    // Kafka will automatically select partition based on key if partition is null.
    return new ProducerRecord<>(destination, null, key, value, headers);
  }

  public GrpcRequest getGrpcMessage() {
    if (type != Type.GRPC) {
      throw new IllegalStateException("this is not a gRPC Kafka message");
    }
    if (key != null) {
      return new GrpcRequest(
          group,
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
          key);
    }
    return new GrpcRequest(
        group,
        topic,
        partition,
        offset,
        stub,
        retryCount,
        dispatchAttempt,
        timeoutCount,
        value,
        physicalTopic,
        physicalCluster,
        physicalPartition,
        physicalOffset,
        headers);
  }

  public Type getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DispatcherMessage that = (DispatcherMessage) o;
    return partition == that.partition
        && offset == that.offset
        && stub == that.stub
        && retryCount == that.retryCount
        && dispatchAttempt == that.dispatchAttempt
        && timeoutCount == that.timeoutCount
        && type == that.type
        && destination.equals(that.destination)
        && Arrays.equals(key, that.key)
        && Arrays.equals(value, that.value)
        && headers.equals(that.headers)
        && group.equals(that.group)
        && topic.equals(that.topic)
        && physicalTopic.equals(that.physicalTopic)
        && physicalCluster.equals(that.physicalCluster)
        && physicalPartition == that.physicalPartition
        && physicalOffset == that.physicalOffset;
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            type,
            destination,
            headers,
            group,
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
            physicalOffset);
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  public String getDestination() {
    return destination;
  }

  // The type of the message to dispatch.
  public enum Type {
    // Kafka dispatcher type sent to a the topic specified in destination.
    KAFKA,
    // GRPC dispatcher is sent to user service via gRPC.
    GRPC;
  }
}
