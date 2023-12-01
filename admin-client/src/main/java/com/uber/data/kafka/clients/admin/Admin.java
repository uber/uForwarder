package com.uber.data.kafka.clients.admin;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

/**
 * Admin interface for interacting with Kafka.
 *
 * <p>This was copied from org.apache.kafka.clients.admin.Admin in Apache Kafka Client 2.4 with
 * additional offsetForTimes ported from org.apache.kafka.clients.consumer.Consumer client b/c
 * monorepo doesn't currently use Kafka client 2.4.
 */
public interface Admin {

  /**
   * @return the properties file for this Admin client.
   *     <p>This can be used to detect properties changes.
   */
  Properties getProperties();

  /**
   * Describe some topics, with the default options.
   *
   * <p>This is a convenience method for #{@link Admin#describeTopics(Collection,
   * DescribeTopicsOptions)} with default options. See the overload for more details.
   *
   * @param topicNames The names of the topics to describe.
   * @return A describe topic result.
   */
  default DescribeTopicsResult describeTopics(Collection<String> topicNames) {
    return describeTopics(topicNames, new DescribeTopicsOptions());
  }

  /**
   * Describe some topics in the cluster.
   *
   * @param topicNames The names of the topics to describe.
   * @param options The options to use when describing the topic.
   * @return A describe topic result.
   */
  DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options);

  /**
   * Look up the offsets for the given partitions by timestamp. The returned offset for each
   * partition is the earliest offset whose timestamp is greater than or equal to the given
   * timestamp in the corresponding partition.
   *
   * <p>This is a blocking call. The consumer does not have to be assigned the partitions. If the
   * message format version in a partition is before 0.10.0, i.e. the messages do not have
   * timestamps, null will be returned for that partition.
   *
   * @param timestampsToSearch the mapping from partition to the timestamp to look up. timestamp is
   *     expressed as milliseconds since epoch.
   * @return a mapping of topic-partition to the timestamp and offset of the first message with
   *     timestamp greater than or equal to the target timestamp. {@code null} will be returned for
   *     the partition if there is no such message.
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
  default Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
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
   * @param timestampsToSearch the mapping from partition to the timestamp to look up. timestamp is
   *     expressed as milliseconds since epoch.
   * @param timeout The maximum amount of time to await retrieval of the offsets
   * @return a mapping of topic-partition to the timestamp and offset of the first message with
   *     timestamp greater than or equal to the target timestamp. {@code null} will be returned for
   *     the partition if there is no such message.
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
  Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch, Duration timeout);

  /**
   * List the consumer groups available in the cluster.
   *
   * @param options The options to use when listing the consumer groups.
   * @return a list consumer groups result.
   */
  ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options);

  /**
   * List the committed offset of a consumer group in a cluster.
   *
   * @param groupId the id of the consumer group
   * @return a Kafka future containing a map of <TopicPartition, OffsetMetadata>
   */
  ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId);

  /**
   * List the consumer groups available in the cluster with the default options.
   *
   * <p>This is a convenience method for #{@link
   * MultiClusterAdmin#listConsumerGroups(ListConsumerGroupsOptions)} with default options. See the
   * overload for more details.
   *
   * @return a list consumer groups result.
   */
  default ListConsumerGroupsResult listConsumerGroups() {
    return listConsumerGroups(new ListConsumerGroupsOptions());
  }

  /**
   * List the topics available in the cluster.
   *
   * @param options The options to use when listing the topics.
   * @return The ListTopicsResult.
   */
  ListTopicsResult listTopics(ListTopicsOptions options);

  /**
   * List the topics available in the cluster with the default options.
   *
   * <p>#{@link Admin#listTopics(ListTopicsOptions)} with default options. See the overload for more
   * details.
   *
   * @return The ListTopicsResult.
   */
  default ListTopicsResult listTopics() {
    return listTopics(new ListTopicsOptions());
  }

  /**
   * Describe some group IDs in the cluster.
   *
   * @param groupIds The IDs of the groups to describe.
   * @param options The options to use when describing the groups.
   * @return a describe consumer groups result.
   */
  DescribeConsumerGroupsResult describeConsumerGroups(
      Collection<String> groupIds, DescribeConsumerGroupsOptions options);

  /**
   * Describe some group IDs in the cluster, with the default options.
   *
   * <p>This is a convenience method for #{@link
   * MultiClusterAdmin#describeConsumerGroups(Collection, DescribeConsumerGroupsOptions)} with
   * default options. See the overload for more details.
   *
   * @param groupIds The IDs of the groups to describe.
   * @return a describe consumer groups result.
   */
  default DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds) {
    return describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions());
  }

  /**
   * Get the beginning offsets for the given partitions. If the partition has never been written to,
   * the beginning offsets are 0.
   *
   * @param partitions the partitions to get the beginning offsets.
   * @return The beginning offsets for the given partitions.
   * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the
   *     exception for more details
   * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
   *     topic(s). See the exception for more details
   * @throws org.apache.kafka.common.errors.TimeoutException if the offsets could not be fetched
   *     before expiration of the passed timeout
   */
  default Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return beginningOffsets(partitions, Duration.ofMinutes(1));
  }

  /**
   * Get the beginning offsets for the given partitions. If the partition has never been written to,
   * the beginning offsets are 0.
   *
   * @param partitions the partitions to get the beginning offsets.
   * @param timeout The maximum amount of time to await retrieval of the beginning offsets
   * @return The beginning offsets for the given partitions.
   * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the
   *     exception for more details
   * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
   *     topic(s). See the exception for more details
   * @throws org.apache.kafka.common.errors.TimeoutException if the offsets could not be fetched
   *     before expiration of the passed timeout
   */
  Map<TopicPartition, Long> beginningOffsets(
      Collection<TopicPartition> partitions, Duration timeout);

  /**
   * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation
   * level, the end offset is the high watermark (that is, the offset of the last successfully
   * replicated message plus one). For {@code read_committed} consumers, the end offset is the last
   * stable offset (LSO), which is the minimum of the high watermark and the smallest offset of any
   * open transaction. Finally, if the partition has never been written to, the end offset is 0.
   *
   * @param partitions the partitions to get the end offsets.
   * @return The end offsets for the given partitions.
   * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the
   *     exception for more details
   * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
   *     topic(s). See the exception for more details
   * @throws org.apache.kafka.common.errors.TimeoutException if the offset metadata could not be
   *     fetched before the amount of time allocated by {@code request.timeout.ms} expires
   */
  default Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    return endOffsets(partitions, Duration.ofMinutes(1));
  }

  /**
   * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation
   * level, the end offset is the high watermark (that is, the offset of the last successfully
   * replicated message plus one). For {@code read_committed} consumers, the end offset is the last
   * stable offset (LSO), which is the minimum of the high watermark and the smallest offset of any
   * open transaction. Finally, if the partition has never been written to, the end offset is 0.
   *
   * @param partitions the partitions to get the end offsets.
   * @param timeout The maximum amount of time to await retrieval of the end offsets
   * @return The end offsets for the given partitions.
   * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the
   *     exception for more details
   * @throws org.apache.kafka.common.errors.AuthorizationException if not authorized to the
   *     topic(s). See the exception for more details
   * @throws org.apache.kafka.common.errors.TimeoutException if the offsets could not be fetched
   *     before expiration of the passed timeout
   */
  Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout);

  /** Closes the Admin client and cleanups any state. */
  void close();
}
