package com.uber.data.kafka.clients.admin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MultiClusterAdminClient collects admin information from multiple Kafka clusters via
 * scatter-gather to individual AdminClients.
 */
final class MultiClusterAdminClient implements MultiClusterAdmin {
  private static final Logger LOGGER = LoggerFactory.getLogger(MultiClusterAdmin.class);
  private final ConcurrentMap<String, Admin> clientMap;

  private final Function<String, Properties> propertiesProvider;
  private final Function<Properties, AdminClient> kafkaAdminClientFactory;
  private final Function<Properties, Consumer<byte[], byte[]>> kafkaConsumerFactory;
  private final boolean isPropertiesProviderIdempotent;

  /**
   * Creates a new MultiClusterAdminClient.
   *
   * @param adminClientMap is a AdminClientMap to seed.
   * @param propertiesProvider is used to create new Apache Kafka properties configuration.
   * @param kafkaAdminClientFactory is used to create new Apache Kafka MultiClusterAdmin clients
   *     when addCluster is invoked.
   * @param kafkaConsumerFactory is used to create new Apache Kafka Consumer clients when addCluster
   *     is invoked.
   *     <p>Please use MultiClusterAdminClient.of() instead.
   */
  @VisibleForTesting
  MultiClusterAdminClient(
      ConcurrentMap<String, Admin> adminClientMap,
      Function<String, Properties> propertiesProvider,
      boolean isPropertiesProviderIdempotent,
      Function<Properties, AdminClient> kafkaAdminClientFactory,
      Function<Properties, Consumer<byte[], byte[]>> kafkaConsumerFactory) {
    this.clientMap = adminClientMap;
    this.propertiesProvider = propertiesProvider;
    this.isPropertiesProviderIdempotent = isPropertiesProviderIdempotent;
    this.kafkaAdminClientFactory = kafkaAdminClientFactory;
    this.kafkaConsumerFactory = kafkaConsumerFactory;
  }

  /**
   * Creates a new MultiClusterAdminClient.
   *
   * @param propertiesProvider the properties provider
   * @param isPropertiesProviderIdempotent indicates if provider is idempotent. If true, it will
   *     prefer cached client without resolve property
   * @return MultiClusterAdminClient with sane defaults configured to connect to real Kafka
   *     clusters.
   */
  static MultiClusterAdminClient of(
      Function<String, Properties> propertiesProvider, boolean isPropertiesProviderIdempotent) {
    return new MultiClusterAdminClient(
        new ConcurrentHashMap<>(),
        propertiesProvider,
        isPropertiesProviderIdempotent,
        AdminClient::create,
        KafkaConsumer::new);
  }

  /**
   * Gets an admin client (lazily creating one if necessary). If creation is necessary, the
   * propertiesProvider is invoked to build the properties file.
   *
   * @param clusterName that the Admin client should be connected to.
   * @return a single cluster Admin client.
   */
  @Override
  public Admin getAdmin(String clusterName) {
    return registerAdminClient(clusterName);
  }

  @Override
  public Map<String, Map<String, KafkaFuture<TopicDescription>>> describeTopics(
      Collection<String> topicNames, DescribeTopicsOptions options) {
    Map<String, Map<String, KafkaFuture<TopicDescription>>> futureMap = new HashMap<>();
    clientMap.forEach(
        (cluster, adminClient) -> {
          futureMap.put(cluster, adminClient.describeTopics(topicNames, options).values());
        });
    return Collections.unmodifiableMap(futureMap);
  }

  @Override
  public Map<String, Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
    Map<String, Map<TopicPartition, OffsetAndTimestamp>> resultMap = new HashMap<>();
    clientMap.forEach(
        (cluster, adminClient) -> {
          resultMap.put(cluster, adminClient.offsetsForTimes(timestampsToSearch, timeout));
        });
    return resultMap;
  }

  @Override
  public Map<String, KafkaFuture<Collection<ConsumerGroupListing>>> listConsumerGroups(
      ListConsumerGroupsOptions options) {
    Map<String, KafkaFuture<Collection<ConsumerGroupListing>>> futureMap = new HashMap<>();
    clientMap.forEach(
        (cluster, adminClient) -> {
          futureMap.put(cluster, adminClient.listConsumerGroups(options).all());
        });
    return Collections.unmodifiableMap(futureMap);
  }

  @Override
  public Map<String, ListConsumerGroupOffsetsResult> listConsumerGroupsOffsets(String groupId) {
    Map<String, ListConsumerGroupOffsetsResult> futureMap = new HashMap<>();
    clientMap.forEach(
        (cluster, adminClient) -> {
          futureMap.put(cluster, adminClient.listConsumerGroupOffsets(groupId));
        });
    return futureMap;
  }

  /**
   * Describes the consumer groups by querying each registered cluster admin client for the provided
   * groupIds.
   *
   * @param groupIds The IDs of the groups to describe.
   * @param options The options to use when describing the groups.
   * @return a mapping of cluster to group to consumer group description future.
   * @implNote since the implementation has no way of knowing which cluster the groupIds belong to,
   *     we query all clusters for all groupIds. As a result, some clusters may return an
   *     exceptional future if that groupId does not exist on that cluster.
   */
  @Override
  public Map<String, Map<String, KafkaFuture<ConsumerGroupDescription>>> describeConsumerGroups(
      Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
    Map<String, Map<String, KafkaFuture<ConsumerGroupDescription>>> futureMap = new HashMap<>();
    clientMap.forEach(
        (cluster, adminClient) -> {
          futureMap.put(
              cluster, adminClient.describeConsumerGroups(groupIds, options).describedGroups());
        });
    return Collections.unmodifiableMap(futureMap);
  }

  /**
   * Gets the list of clusters that that admin client is connected to.
   *
   * @return a collection of cluster names.
   */
  @VisibleForTesting
  Collection<String> getClusters() {
    return clientMap.keySet();
  }

  private Admin newSingleClusterAdminClient(Properties props) {
    return new SingleClusterAdminClient(
        props, kafkaAdminClientFactory.apply(props), kafkaConsumerFactory.apply(props));
  }

  private Admin registerAdminClient(String clusterName) {
    return clientMap.compute(
        clusterName,
        (k, v) -> {
          if (v == null) {
            return newSingleClusterAdminClient(propertiesProvider.apply(clusterName));
          } else if (isPropertiesProviderIdempotent) {
            // return cached client if property provider is idempotent
            return v;
          }

          // otherwise, replace cached client with new client
          Properties properties = propertiesProvider.apply(clusterName);
          Properties oldProperties = v.getProperties();
          if (!isEqual(oldProperties, properties)) {
            try {
              v.close();
            } catch (RuntimeException e) {
              LOGGER.warn("failed to cleanly close admin client for {}", clusterName, e);
            }
            return newSingleClusterAdminClient(properties);
          }

          return v;
        });
  }

  /**
   * Returns true if prop1 and prop2 have the same key value pairs, otherwise returns false.
   *
   * @implNote we implement a separate comparer so that we can log the difference.
   */
  @VisibleForTesting
  static boolean isEqual(Properties oldProps, Properties newProps) {
    Set<Map.Entry<Object, Object>> oldEntries = oldProps.entrySet();
    Set<Map.Entry<Object, Object>> newEntries = newProps.entrySet();
    Sets.SetView<Map.Entry<Object, Object>> added = Sets.difference(newEntries, oldEntries);
    Sets.SetView<Map.Entry<Object, Object>> removed = Sets.difference(oldEntries, newEntries);

    ImmutableList.Builder<String> addedStrings = ImmutableList.builder();
    for (Map.Entry<Object, Object> add : added) {
      addedStrings.add(String.format("%s:%s", add.getKey().toString(), add.getValue().toString()));
    }

    ImmutableList.Builder<String> removedStrings = ImmutableList.builder();
    for (Map.Entry<Object, Object> remove : removed) {
      addedStrings.add(
          String.format("%s:%s", remove.getKey().toString(), remove.getValue().toString()));
    }

    boolean equal = added.size() == 0 && removed.size() == 0;
    if (!equal) {
      LOGGER.info(
          "reload admin client due to properties change: added [{}], removed [{}]",
          String.join(",", addedStrings.build()),
          String.join(",", removedStrings.build()));
    }
    return equal;
  }
}
