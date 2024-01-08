package com.uber.data.kafka.datatransfer.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

/** Wrapper of Kafka admit client {@link org.apache.kafka.clients.admin.AdminClient} */
public class AdminClient {
  private static final DescribeTopicsOptions DESCRIBE_TOPICS_OPTIONS = new DescribeTopicsOptions();
  private final org.apache.kafka.clients.admin.AdminClient delegator;

  private AdminClient(Properties properties) {
    this.delegator = org.apache.kafka.clients.admin.AdminClient.create(properties);
  }

  protected AdminClient(org.apache.kafka.clients.admin.AdminClient delegator) {
    this.delegator = delegator;
  }

  /**
   * Lists the topics available in the cluster with the default options.
   *
   * @return The ListTopicsResult.
   */
  public ListTopicsResult listTopics() {
    return delegator.listTopics();
  }

  /**
   * Describes some topics in the cluster with the default options
   *
   * @param topicNames – The names of the topics to describe.
   * @return The DescribeTopicsResult.
   */
  public DescribeTopicsResult describeTopics(Collection<String> topicNames) {
    return delegator.describeTopics(topicNames, DESCRIBE_TOPICS_OPTIONS);
  }

  /**
   * Queries offsets of topic partitions by timestamp
   *
   * @param timestampsToSearch the timestamps to search
   * @return The ListOffsetsResult
   */
  public ListOffsetsResult offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
    Map<TopicPartition, OffsetSpec> topicPartitionOffsets =
        timestampsToSearch
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey(), entry -> OffsetSpec.forTimestamp(entry.getValue())));
    return delegator.listOffsets(topicPartitionOffsets);
  }

  /**
   * Queries earliest offsets of topic partitions
   *
   * @param partitions the partitions
   * @return The ListOffsetsResult
   */
  public ListOffsetsResult beginningOffsets(Collection<TopicPartition> partitions) {
    return delegator.listOffsets(toOffsetSpecMap(partitions, OffsetSpec.earliest()));
  }

  /**
   * Queries latest offsets of topic partitions
   *
   * @param partitions the partitions
   * @return The ListOffsetsResult
   */
  public ListOffsetsResult endOffsets(Collection<TopicPartition> partitions) {
    return delegator.listOffsets(toOffsetSpecMap(partitions, OffsetSpec.latest()));
  }

  /**
   * List the consumer group offsets available in the cluster with the default options.
   *
   * @param groupId the consumer group id
   * @return The ListConsumerGroupOffsetsResult
   */
  public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
    return delegator.listConsumerGroupOffsets(groupId);
  }

  /**
   * Creates builder instance
   *
   * @param propertiesProvider the properties provider
   * @return the builder
   */
  public static Builder newBuilder(Function<String, Properties> propertiesProvider) {
    return new Builder(propertiesProvider);
  }

  private Map<TopicPartition, OffsetSpec> toOffsetSpecMap(
      Collection<TopicPartition> topicPartitions, OffsetSpec offsetSpec) {
    return topicPartitions
        .stream()
        .collect(Collectors.toMap(Function.identity(), entry -> offsetSpec));
  }

  /** Builder of AdminClient that caches clients by cluster name */
  public static class Builder {
    private final Function<String, Properties> propertiesProvider;
    private final Map<String, AdminClient> clientCache = new HashMap<>();

    private Builder(Function<String, Properties> propertiesProvider) {
      this.propertiesProvider = propertiesProvider;
    }

    /**
     * Builds AdminClient it also caches AdminClient by cluster name
     *
     * @param cluster the cluster
     * @return The AdminClient
     */
    public AdminClient build(String cluster) {
      return clientCache.computeIfAbsent(
          cluster, c -> new AdminClient(propertiesProvider.apply(c)));
    }
  }
}
