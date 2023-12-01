package com.uber.data.kafka.consumerproxy.worker.processor;

import java.util.Objects;

final class TopicPartitionOffset {
  private final String topic;
  private final int partition;
  private final long offset;

  TopicPartitionOffset(String topic, int partition, long offset) {
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TopicPartitionOffset that = (TopicPartitionOffset) o;
    return partition == that.partition
        && offset == that.offset
        && Objects.equals(topic, that.topic);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, partition, offset);
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
}
