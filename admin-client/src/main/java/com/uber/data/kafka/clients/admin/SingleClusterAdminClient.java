package com.uber.data.kafka.clients.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;

/**
 * SingleClusterAdminClient implements the single cluster Admin interface.
 *
 * <p>We wrap Apache AdminClient and Consumer b/c we want need Consumer to implement offsetForTimes.
 * In the future, this will be pulled into Apache Kafka Admin Client so we can deprecate this class
 * then.
 */
final class SingleClusterAdminClient implements Admin {
  private static final long KAFKA_CONSUMER_LOCK_TIMEOUT_IN_MS = 10000L;
  private final Properties properties;
  private final AdminClient adminClient;
  private final Consumer<byte[], byte[]> kafkaConsumer;
  private final ReentrantLock kafkaConsumerMutexLock;

  SingleClusterAdminClient(
      Properties properties, AdminClient adminClient, Consumer<byte[], byte[]> kafkaConsumer) {
    this.properties = properties;
    this.adminClient = adminClient;
    this.kafkaConsumer = kafkaConsumer;
    this.kafkaConsumerMutexLock = new ReentrantLock();
  }

  @Override
  public Properties getProperties() {
    return properties;
  }

  @Override
  public DescribeTopicsResult describeTopics(
      Collection<String> topicNames, DescribeTopicsOptions options) {
    return adminClient.describeTopics(topicNames, options);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
    return execFuncWithLock(() -> kafkaConsumer.offsetsForTimes(timestampsToSearch, timeout));
  }

  @Override
  public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
    return adminClient.listConsumerGroups(options);
  }

  @Override
  public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
    return adminClient.listConsumerGroupOffsets(groupId);
  }

  @Override
  public ListTopicsResult listTopics(ListTopicsOptions options) {
    return adminClient.listTopics(options);
  }

  @Override
  public DescribeConsumerGroupsResult describeConsumerGroups(
      Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
    return adminClient.describeConsumerGroups(groupIds, options);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(
      Collection<TopicPartition> partitions, Duration timeout) {
    return execFuncWithLock(() -> kafkaConsumer.beginningOffsets(partitions, timeout));
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(
      Collection<TopicPartition> partitions, Duration timeout) {
    return execFuncWithLock(() -> kafkaConsumer.endOffsets(partitions, timeout));
  }

  private <T> T execFuncWithLock(Supplier<T> func) {
    try {
      if (kafkaConsumerMutexLock.tryLock(
          KAFKA_CONSUMER_LOCK_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS)) {
        try {
          return func.get();
        } finally {
          kafkaConsumerMutexLock.unlock();
        }
      } else {
        throw new TimeoutException("failed to acquire kafkaConsumer lock within timeout");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TimeoutException(e);
    }
  }

  @Override
  public void close() {
    adminClient.close();
    kafkaConsumer.wakeup();
    kafkaConsumer.close();
  }
}
