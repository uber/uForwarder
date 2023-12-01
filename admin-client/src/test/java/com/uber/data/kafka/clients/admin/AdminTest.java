package com.uber.data.kafka.clients.admin;

import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class AdminTest extends FievelTestBase {
  private AtomicLong callCount;
  private Admin admin;

  @Before
  public void setup() {
    callCount = new AtomicLong();
    admin =
        new Admin() {
          @Override
          public void close() {}

          @Override
          public Properties getProperties() {
            return new Properties();
          }

          @Override
          public DescribeTopicsResult describeTopics(
              Collection<String> topicNames, DescribeTopicsOptions options) {
            callCount.incrementAndGet();
            DescribeTopicsResult result = Mockito.mock(DescribeTopicsResult.class);
            return result;
          }

          @Override
          public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
              Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
            callCount.incrementAndGet();
            return new HashMap<>();
          }

          @Override
          public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
            callCount.incrementAndGet();
            ListConsumerGroupsResult result = Mockito.mock(ListConsumerGroupsResult.class);
            return result;
          }

          @Override
          public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId) {
            callCount.incrementAndGet();
            ListConsumerGroupOffsetsResult result =
                Mockito.mock(ListConsumerGroupOffsetsResult.class);
            return result;
          }

          @Override
          public ListTopicsResult listTopics(ListTopicsOptions options) {
            callCount.incrementAndGet();
            ListTopicsResult result = Mockito.mock(ListTopicsResult.class);
            return result;
          }

          @Override
          public DescribeConsumerGroupsResult describeConsumerGroups(
              Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
            callCount.incrementAndGet();
            DescribeConsumerGroupsResult result = Mockito.mock(DescribeConsumerGroupsResult.class);
            return result;
          }

          @Override
          public Map<TopicPartition, Long> beginningOffsets(
              Collection<TopicPartition> partitions, Duration timeout) {
            callCount.incrementAndGet();
            return new HashMap<>();
          }

          @Override
          public Map<TopicPartition, Long> endOffsets(
              Collection<TopicPartition> partitions, Duration timeout) {
            callCount.incrementAndGet();
            return new HashMap<>();
          }
        };
  }

  @Test
  public void testDescribeTopics() {
    admin.describeTopics(new ArrayList<>());
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testOffsetForTimes() {
    admin.offsetsForTimes(new HashMap<>());
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testListConsumerGroups() {
    admin.listConsumerGroups();
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testListTopics() {
    admin.listTopics();
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testDescribeConsumerGroups() {
    admin.describeConsumerGroups(new ArrayList<>());
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testBeginningOffsets() {
    admin.beginningOffsets(new ArrayList<>());
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testEndOffsets() {
    admin.endOffsets(new ArrayList<>());
    Assert.assertEquals(1, callCount.get());
  }
}
