package com.uber.data.kafka.clients.admin;

import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MultiClusterAdminClientTest extends FievelTestBase {
  private static final String GROUP = "group";
  private static final String CLUSTER = "cluster";
  private static final String TOPIC = "topic";
  private ConcurrentMap<String, Admin> adminClientMap;
  private Consumer<byte[], byte[]> consumer;
  private AdminClient adminClient;
  private MultiClusterAdminClient multiClusterAdminClient;
  private Properties properties;
  private Properties modifiedProperties;
  private Function<String, Properties> propertiesFunction;

  @Before
  public void setup() {
    properties = new Properties();
    modifiedProperties = new Properties();
    modifiedProperties.put("key", "value");

    adminClient = Mockito.mock(AdminClient.class);
    // assign mock to local variable so that we can Suppress unchecked to smallest scope.
    @SuppressWarnings("unchecked")
    Consumer<byte[], byte[]> mockConsumer = Mockito.mock(Consumer.class);
    consumer = mockConsumer;

    @SuppressWarnings("unchecked")
    Function<String, Properties> f = Mockito.mock(Function.class);
    propertiesFunction = f;
    Mockito.when(propertiesFunction.apply(CLUSTER))
        .thenReturn(properties)
        .thenReturn(properties)
        .thenReturn(modifiedProperties)
        .thenReturn(properties);

    adminClientMap = new ConcurrentHashMap<>();
    multiClusterAdminClient =
        new MultiClusterAdminClient(
            adminClientMap, propertiesFunction, false, props -> adminClient, props -> consumer);

    Assert.assertEquals(0, multiClusterAdminClient.getClusters().size());
    multiClusterAdminClient.getAdmin(CLUSTER);
    Assert.assertEquals(1, multiClusterAdminClient.getClusters().size());
  }

  @Test
  public void testGetAdmin() throws Exception {
    Admin singleClusterAdminClient = multiClusterAdminClient.getAdmin(CLUSTER);
    Assert.assertEquals(properties, singleClusterAdminClient.getProperties());

    singleClusterAdminClient = multiClusterAdminClient.getAdmin(CLUSTER);
    Assert.assertEquals(modifiedProperties, singleClusterAdminClient.getProperties());

    Mockito.doThrow(new RuntimeException()).when(adminClient).close();
    multiClusterAdminClient.getAdmin(CLUSTER);
  }

  @Test
  public void testGetAdminWitIdempotentPropertiesProvider() throws Exception {
    multiClusterAdminClient =
        new MultiClusterAdminClient(
            adminClientMap, propertiesFunction, true, props -> adminClient, props -> consumer);

    Mockito.verify(propertiesFunction, Mockito.times(1)).apply(CLUSTER);
    Admin singleClusterAdminClient = multiClusterAdminClient.getAdmin(CLUSTER);
    Assert.assertEquals(properties, singleClusterAdminClient.getProperties());
    // doesn't get property when get client
    Mockito.verify(propertiesFunction, Mockito.times(1)).apply(CLUSTER);
  }

  @Test
  public void testDescribeTopics() throws Exception {
    // setup mock return value in a verbose way b/c we do not want to import guava collections.
    List<TopicPartitionInfo> topicPartitionInfoList =
        Stream.of(
                new TopicPartitionInfo(0, Node.noNode(), new ArrayList<>(), new ArrayList<>()),
                new TopicPartitionInfo(1, Node.noNode(), new ArrayList<>(), new ArrayList<>()))
            .collect(Collectors.toList());
    TopicDescription topicDescription = new TopicDescription(TOPIC, false, topicPartitionInfoList);
    KafkaFutureImpl<TopicDescription> topicDescriptionFuture = new KafkaFutureImpl<>();
    topicDescriptionFuture.complete(topicDescription);
    Map<String, KafkaFuture<TopicDescription>> topicDescriptionMap = new HashMap<>();
    topicDescriptionMap.put(TOPIC, topicDescriptionFuture);
    DescribeTopicsResult describeTopicsResult = Mockito.mock(DescribeTopicsResult.class);
    Mockito.when(describeTopicsResult.values()).thenReturn(topicDescriptionMap);

    List<String> topicsToQuery = Stream.of(TOPIC).collect(Collectors.toList());
    DescribeTopicsOptions options = new DescribeTopicsOptions();
    Mockito.when(adminClient.describeTopics(topicsToQuery, options))
        .thenReturn(describeTopicsResult);

    Map<String, Map<String, KafkaFuture<TopicDescription>>> multiClusterResult =
        multiClusterAdminClient.describeTopics(topicsToQuery, options);
    Assert.assertEquals(1, multiClusterResult.size());
    Map<String, KafkaFuture<TopicDescription>> singleClusterResult =
        multiClusterResult.get(CLUSTER);
    Assert.assertEquals(1, singleClusterResult.size());
    KafkaFuture<TopicDescription> singleTopicResult = singleClusterResult.get(TOPIC);
    TopicDescription topicDescriptionResult = singleTopicResult.get();
    Assert.assertEquals(topicDescription, topicDescriptionResult);
  }

  @Test
  public void testOffsetsForTimes() throws Exception {
    TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
    Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
    timestampToSearch.put(topicPartition, 200L);

    Map<TopicPartition, OffsetAndTimestamp> timestampToOffsetMap = new HashMap<>();
    timestampToOffsetMap.put(topicPartition, new OffsetAndTimestamp(100, 200));

    Mockito.when(consumer.offsetsForTimes(timestampToSearch, Duration.ofSeconds(1)))
        .thenReturn(timestampToOffsetMap);

    Map<String, Map<TopicPartition, OffsetAndTimestamp>> multiClusterResult =
        multiClusterAdminClient.offsetsForTimes(timestampToSearch, Duration.ofSeconds(1));
    Map<TopicPartition, OffsetAndTimestamp> singleClusterResult = multiClusterResult.get(CLUSTER);
    Assert.assertEquals(timestampToOffsetMap, singleClusterResult);
  }

  @Test
  public void testListConsumerGroups() throws Exception {
    ConsumerGroupListing consumerGroup = new ConsumerGroupListing(GROUP, false);
    List<ConsumerGroupListing> consumerGroupList =
        Stream.of(consumerGroup).collect(Collectors.toList());
    KafkaFutureImpl<Collection<ConsumerGroupListing>> consumerGroupFuture = new KafkaFutureImpl<>();
    consumerGroupFuture.complete(consumerGroupList);
    ListConsumerGroupsResult listConsumerGroupsResult =
        Mockito.mock(ListConsumerGroupsResult.class);
    Mockito.when(listConsumerGroupsResult.all()).thenReturn(consumerGroupFuture);
    Mockito.when(adminClient.listConsumerGroups(Mockito.any()))
        .thenReturn(listConsumerGroupsResult);

    Map<String, KafkaFuture<Collection<ConsumerGroupListing>>> multiClusterResult =
        multiClusterAdminClient.listConsumerGroups(new ListConsumerGroupsOptions());
    KafkaFuture<Collection<ConsumerGroupListing>> singleClusterResult =
        multiClusterResult.get(CLUSTER);
    Assert.assertEquals(consumerGroupList, singleClusterResult.get());
  }

  @Test
  public void testDescribeConsumerGroups() throws Exception {
    List<String> consumerGroups = Stream.of(GROUP).collect(Collectors.toList());
    ConsumerGroupDescription consumerGroupDescription =
        new ConsumerGroupDescription(
            GROUP,
            false,
            new ArrayList<>(),
            "partitionAssigner",
            ConsumerGroupState.STABLE,
            Node.noNode());
    KafkaFutureImpl<ConsumerGroupDescription> consumerGroupDescriptionFuture =
        new KafkaFutureImpl<>();
    consumerGroupDescriptionFuture.complete(consumerGroupDescription);
    Map<String, KafkaFuture<ConsumerGroupDescription>> consumerGroupDescriptionMap =
        new HashMap<>();
    consumerGroupDescriptionMap.put(GROUP, consumerGroupDescriptionFuture);
    DescribeConsumerGroupsResult describeConsumerGroupsResult =
        Mockito.mock(DescribeConsumerGroupsResult.class);
    Mockito.when(describeConsumerGroupsResult.describedGroups())
        .thenReturn(consumerGroupDescriptionMap);
    DescribeConsumerGroupsOptions options = new DescribeConsumerGroupsOptions();
    Mockito.when(adminClient.describeConsumerGroups(consumerGroups, options))
        .thenReturn(describeConsumerGroupsResult);

    Map<String, Map<String, KafkaFuture<ConsumerGroupDescription>>> multiClusterResult =
        multiClusterAdminClient.describeConsumerGroups(consumerGroups, options);
    Map<String, KafkaFuture<ConsumerGroupDescription>> singleClusterResult =
        multiClusterResult.get(CLUSTER);
    KafkaFuture<ConsumerGroupDescription> singleGroupResult = singleClusterResult.get(GROUP);
    Assert.assertEquals(consumerGroupDescription, singleGroupResult.get());
  }

  @Test
  public void testIsEqual() {
    Properties prop1 = new Properties();
    Properties prop2 = new Properties();
    Assert.assertTrue(MultiClusterAdminClient.isEqual(prop1, prop2));

    prop2.setProperty("key1", "value1");
    Assert.assertFalse(MultiClusterAdminClient.isEqual(prop1, prop2));

    prop1.setProperty("key1", "value1");
    Assert.assertTrue(MultiClusterAdminClient.isEqual(prop1, prop2));

    prop1.setProperty("key2", "value2");
    Assert.assertFalse(MultiClusterAdminClient.isEqual(prop1, prop2));

    prop2.setProperty("key2", "value2");
    Assert.assertTrue(MultiClusterAdminClient.isEqual(prop1, prop2));
  }
}
