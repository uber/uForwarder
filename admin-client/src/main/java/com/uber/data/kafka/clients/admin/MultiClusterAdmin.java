package com.uber.data.kafka.clients.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

/**
 * MultiClusterAdmin is an admin interface for accessing Uber Kafka Clusters.
 *
 * @implNote As much as possible, we align the interface to be as close to {@link
 *     org.apache.kafka.clients.admin.AdminClient} so that we can easily use an adapter to implement
 *     API compatibility with {@link org.apache.kafka.clients.admin.AdminClient}. A consequence of
 *     this goal is that:
 *     <ol>
 *       <li>Types provided by in {@link org.apache.kafka.clients.admin} should be used.
 *       <li>Return values should generally be a map of cluster to open source admin client return
 *           type: {@code Map<String, <open_source_return_type>>}
 *     </ol>
 */
public interface MultiClusterAdmin extends MultiClusterAdminFactory {

  /**
   * Creates a new MultiClusterAdmin client that is connected to each of the kafka clusters that was
   * registered.
   *
   * @param propertiesProvider that is invoked to generate the properties file for a cluster at if
   *     the MultiClusterAdminClient needs to load a new AdminClient. Properties must have the
   *     necessary fields to initialize a Kafka Admin Client
   *     https://kafka.apache.org/documentation/#adminclientconfigs and Kafka Consumer Client
   *     https://kafka.apache.org/documentation/#consumerconfigs to the respective clusters.
   * @return The new MultiClusterAdminClient.
   */
  static MultiClusterAdmin create(Function<String, Properties> propertiesProvider) {
    return create(propertiesProvider, false);
  }

  /**
   * Creates a new MultiClusterAdmin client that is connected to each of the kafka clusters that was
   * registered.
   *
   * @param propertiesProvider that is invoked to generate the properties file for a cluster at if
   *     the MultiClusterAdminClient needs to load a new AdminClient. Properties must have the
   *     necessary fields to initialize a Kafka Admin Client
   *     https://kafka.apache.org/documentation/#adminclientconfigs and Kafka Consumer Client
   *     https://kafka.apache.org/documentation/#consumerconfigs to the respective clusters.
   * @param isPropertiesProviderIdempotent indicates the property provider is idempotent. If true,
   *     it will prefer cached client without resolve property
   * @return The new MultiClusterAdminClient.
   */
  static MultiClusterAdmin create(
      Function<String, Properties> propertiesProvider, boolean isPropertiesProviderIdempotent) {
    return MultiClusterAdminClient.of(propertiesProvider, isPropertiesProviderIdempotent);
  }

  /**
   * Describe some topics, with the default options.
   *
   * <p>This is a convenience method for #{@link MultiClusterAdmin#describeTopics(Collection,
   * DescribeTopicsOptions)} with default options. See the overload for more details.
   *
   * @param topicNames The names of the topics to describe.
   * @return A map of cluster to topic to TopicDescriptions.
   */
  default Map<String, Map<String, KafkaFuture<TopicDescription>>> describeTopics(
      Collection<String> topicNames) {
    return describeTopics(topicNames, new DescribeTopicsOptions());
  }

  /**
   * Describe some topics in the cluster.
   *
   * @param topicNames The names of the topics to describe.
   * @param options The options to use when describing the topic.
   * @return A map of cluster to topic to TopicDescriptions.
   */
  Map<String, Map<String, KafkaFuture<TopicDescription>>> describeTopics(
      Collection<String> topicNames, DescribeTopicsOptions options);

  /**
   * Look up the offsets for the given partitions by timestamp. The returned offset for each
   * partition is the earliest offset whose timestamp is greater than or equal to the given
   * timestamp in the corresponding partition.
   *
   * <p>This is a blocking call. The consumer does not have to be assigned the partitions. If the
   * message format version in a partition is before 0.10.0, i.e. the messages do not have
   * timestamps, null will be returned for that partition.
   *
   * @param timestampsToSearch the mapping from partition to the timestamp to look up.
   * @return a mapping from cluster to topic-partition to the timestamp and offset of the first
   *     message with timestamp greater than or equal to the target timestamp. {@code null} will be
   *     returned for the partition if there is no such message.
   * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the
   *     exception for more details
   * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
   *     topic(s). See the exception for more details
   * @throws IllegalArgumentException if the target timestamp is negative
   * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
   *     fetched before the amount of time allocated by {@code default.api.timeout.ms} expires.
   * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not
   *     support looking up the offsets by timestamp
   */
  default Map<String, Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch) {
    return offsetsForTimes(timestampsToSearch, Duration.ofMillis(Long.MAX_VALUE));
  }

  /**
   * Look up the offsets for the given partitions by timestamp. The returned offset for each
   * partition is the earliest offset whose timestamp is greater than or equal to the given
   * timestamp in the corresponding partition.
   *
   * <p>This is a blocking call. The consumer does not have to be assigned the partitions. If the
   * message format version in a partition is before 0.10.0, i.e. the messages do not have
   * timestamps, null will be returned for that partition.
   *
   * @param timestampsToSearch the mapping from partition to the timestamp to look up.
   * @param timeout The maximum amount of time to await retrieval of the offsets
   * @return a mapping from cluster to topic-partition to the timestamp and offset of the first
   *     message with timestamp greater than or equal to the target timestamp. {@code null} will be
   *     returned for the partition if there is no such message.
   * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the
   *     exception for more details
   * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
   *     topic(s). See the exception for more details
   * @throws IllegalArgumentException if the target timestamp is negative
   * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
   *     fetched before expiration of the passed timeout
   * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not
   *     support looking up the offsets by timestamp
   */
  Map<String, Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch, Duration timeout);

  /**
   * List the consumer groups available in the cluster.
   *
   * @param options The options to use when listing the consumer groups.
   * @return a mapping of cluster to consumer group listings.
   */
  Map<String, KafkaFuture<Collection<ConsumerGroupListing>>> listConsumerGroups(
      ListConsumerGroupsOptions options);

  /**
   * List the consumer groups available in the cluster.
   *
   * @param groupId the id of the consumer group
   * @return a mapping of cluster to consumer group offsets listings
   */
  Map<String, ListConsumerGroupOffsetsResult> listConsumerGroupsOffsets(String groupId);

  /**
   * List the consumer groups available in the cluster with the default options.
   *
   * <p>This is a convenience method for #{@link
   * MultiClusterAdmin#listConsumerGroups(ListConsumerGroupsOptions)} with default options. See the
   * overload for more details.
   *
   * @return a mapping of cluster to consumer group listings.
   */
  default Map<String, KafkaFuture<Collection<ConsumerGroupListing>>> listConsumerGroups() {
    return listConsumerGroups(new ListConsumerGroupsOptions());
  }

  /**
   * Describe some group IDs in the cluster.
   *
   * @param groupIds The IDs of the groups to describe.
   * @param options The options to use when describing the groups.
   * @return mapping of cluster to consumer group to ConsumerGroupDescription.
   */
  Map<String, Map<String, KafkaFuture<ConsumerGroupDescription>>> describeConsumerGroups(
      Collection<String> groupIds, DescribeConsumerGroupsOptions options);

  /**
   * Describe some group IDs in the cluster, with the default options.
   *
   * <p>This is a convenience method for #{@link
   * MultiClusterAdmin#describeConsumerGroups(Collection, DescribeConsumerGroupsOptions)} with
   * default options. See the overload for more details.
   *
   * @param groupIds The IDs of the groups to describe.
   * @return mapping of cluster to consumer group to ConsumerGroupDescription.
   */
  default Map<String, Map<String, KafkaFuture<ConsumerGroupDescription>>> describeConsumerGroups(
      Collection<String> groupIds) {
    return describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions());
  }
}
