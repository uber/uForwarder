package com.uber.data.kafka.clients.admin;

import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SingleClusterAdminClientTest extends FievelTestBase {
  private Properties properties;
  private AdminClient adminClient;
  private Consumer<byte[], byte[]> kafkaConsumer;
  private SingleClusterAdminClient singleClusterAdminClient;

  @Before
  public void setup() throws Exception {
    properties = new Properties();
    adminClient = Mockito.mock(AdminClient.class);
    @SuppressWarnings("unchecked")
    Consumer<byte[], byte[]> kafkaConsumer = Mockito.mock(Consumer.class);
    this.kafkaConsumer = kafkaConsumer;
    singleClusterAdminClient = new SingleClusterAdminClient(properties, adminClient, kafkaConsumer);
  }

  @Test
  public void testGetProperties() {
    Assert.assertEquals(properties, singleClusterAdminClient.getProperties());
  }

  @Test
  public void testDescribeTopics() {
    DescribeTopicsResult result = Mockito.mock(DescribeTopicsResult.class);
    List<String> topics = Stream.of("topic").collect(Collectors.toList());
    DescribeTopicsOptions options = new DescribeTopicsOptions();
    Mockito.when(adminClient.describeTopics(topics, options)).thenReturn(result);
    Assert.assertEquals(result, singleClusterAdminClient.describeTopics(topics, options));
  }

  @Test
  public void testOffsetsForTimes() {
    Map<TopicPartition, OffsetAndTimestamp> expected = new HashMap<>();
    expected.put(new TopicPartition("topic", 0), new OffsetAndTimestamp(1, 2));
    Map<TopicPartition, Long> query = new HashMap<>();
    query.put(new TopicPartition("topic", 0), 2L);
    Mockito.when(kafkaConsumer.offsetsForTimes(query, Duration.ofMinutes(1))).thenReturn(expected);
    Assert.assertEquals(
        expected, singleClusterAdminClient.offsetsForTimes(query, Duration.ofMinutes(1)));
  }

  @Test
  public void testListConsumerGroups() {
    ListConsumerGroupsResult result = Mockito.mock(ListConsumerGroupsResult.class);
    ListConsumerGroupsOptions options = new ListConsumerGroupsOptions();
    Mockito.when(adminClient.listConsumerGroups(options)).thenReturn(result);
    Assert.assertEquals(result, singleClusterAdminClient.listConsumerGroups(options));
  }

  @Test
  public void testListConsumerGroupOffsets() {
    ListConsumerGroupOffsetsResult result = Mockito.mock(ListConsumerGroupOffsetsResult.class);
    Mockito.when(adminClient.listConsumerGroupOffsets("group")).thenReturn(result);
    Assert.assertEquals(result, singleClusterAdminClient.listConsumerGroupOffsets("group"));
  }

  @Test
  public void testListTopics() {
    ListTopicsResult result = Mockito.mock(ListTopicsResult.class);
    ListTopicsOptions options = new ListTopicsOptions();
    Mockito.when(adminClient.listTopics(options)).thenReturn(result);
    Assert.assertEquals(result, singleClusterAdminClient.listTopics(options));
  }

  @Test
  public void testDescribeConsumerGroups() {
    DescribeConsumerGroupsResult result = Mockito.mock(DescribeConsumerGroupsResult.class);
    List<String> groups = Stream.of("group").collect(Collectors.toList());
    DescribeConsumerGroupsOptions options = new DescribeConsumerGroupsOptions();
    Mockito.when(adminClient.describeConsumerGroups(groups, options)).thenReturn(result);
    Assert.assertEquals(result, singleClusterAdminClient.describeConsumerGroups(groups, options));
  }

  @Test
  public void testBeginningOffsets() {
    Map<TopicPartition, Long> expected = new HashMap<>();
    expected.put(new TopicPartition("topic", 0), 1L);
    List<TopicPartition> query = new ArrayList<>();
    query.add(new TopicPartition("topic", 0));
    Mockito.when(kafkaConsumer.beginningOffsets(query, Duration.ofMinutes(1))).thenReturn(expected);
    Assert.assertEquals(
        expected, singleClusterAdminClient.beginningOffsets(query, Duration.ofMinutes(1)));
  }

  @Test
  public void testEndOffsets() {
    Map<TopicPartition, Long> expected = new HashMap<>();
    expected.put(new TopicPartition("topic", 0), 1L);
    List<TopicPartition> query = new ArrayList<>();
    query.add(new TopicPartition("topic", 0));
    Mockito.when(kafkaConsumer.endOffsets(query, Duration.ofMinutes(1))).thenReturn(expected);
    Assert.assertEquals(
        expected, singleClusterAdminClient.endOffsets(query, Duration.ofMinutes(1)));
  }

  @Test
  public void testClose() {
    singleClusterAdminClient.close();
    Mockito.verify(adminClient, Mockito.times(1)).close();
    Mockito.verify(kafkaConsumer, Mockito.times(1)).wakeup();
    Mockito.verify(kafkaConsumer, Mockito.times(1)).close();
  }
}
