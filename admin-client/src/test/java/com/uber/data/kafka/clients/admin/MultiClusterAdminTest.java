package com.uber.data.kafka.clients.admin;

import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MultiClusterAdminTest extends FievelTestBase {
  private AtomicLong callCount;
  private MultiClusterAdmin multiClusterAdmin;
  private Admin singleClusterAdmin;

  @Before
  public void setup() {
    callCount = new AtomicLong();
    singleClusterAdmin = Mockito.mock(Admin.class);
    multiClusterAdmin =
        new MultiClusterAdmin() {
          @Override
          public Admin getAdmin(String clusterName) {
            callCount.incrementAndGet();
            return singleClusterAdmin;
          }

          @Override
          public Map<String, Map<String, KafkaFuture<TopicDescription>>> describeTopics(
              Collection<String> topicNames, DescribeTopicsOptions options) {
            callCount.incrementAndGet();
            return new HashMap<>();
          }

          @Override
          public Map<String, Map<TopicPartition, OffsetAndTimestamp>> offsetsForTimes(
              Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
            callCount.incrementAndGet();
            return new HashMap<>();
          }

          @Override
          public Map<String, KafkaFuture<Collection<ConsumerGroupListing>>> listConsumerGroups(
              ListConsumerGroupsOptions options) {
            callCount.incrementAndGet();
            return new HashMap<>();
          }

          @Override
          public Map<String, ListConsumerGroupOffsetsResult> listConsumerGroupsOffsets(
              String groupId) {
            callCount.incrementAndGet();
            return new HashMap<>();
          }

          @Override
          public Map<String, Map<String, KafkaFuture<ConsumerGroupDescription>>>
              describeConsumerGroups(
                  Collection<String> groupIds, DescribeConsumerGroupsOptions options) {
            callCount.incrementAndGet();
            return new HashMap<>();
          }
        };
  }

  @Test
  public void testDescribeTopics() {
    multiClusterAdmin.offsetsForTimes(new HashMap<>());
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testOffsetsForTimes() {
    multiClusterAdmin.describeTopics(Stream.of("topic").collect(Collectors.toList()));
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testListConsumerGroups() {
    multiClusterAdmin.listConsumerGroups();
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testDescribeConsumerGroups() {
    multiClusterAdmin.describeConsumerGroups(Stream.of("group").collect(Collectors.toList()));
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void testCreate() {
    MultiClusterAdmin.create(
        p -> {
          throw new RuntimeException();
        });
  }
}
