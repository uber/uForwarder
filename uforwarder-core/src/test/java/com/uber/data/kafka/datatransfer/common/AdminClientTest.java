package com.uber.data.kafka.datatransfer.common;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class AdminClientTest extends FievelTestBase {
  private AdminClient AdminClient;
  private org.apache.kafka.clients.admin.AdminClient delegator;
  private AdminClient.Builder builder;
  private Function<String, Properties> propertyProvider;

  @Before
  public void setUp() {
    propertyProvider = Mockito.mock(Function.class);
    delegator = Mockito.mock(org.apache.kafka.clients.admin.AdminClient.class);
    builder = AdminClient.newBuilder(propertyProvider);
    AdminClient = new AdminClient(delegator);
  }

  @Test
  public void testBuild() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:1000");
    Mockito.when(propertyProvider.apply("c1")).thenReturn(properties);
    AdminClient adapter1 = builder.build("c1");
    AdminClient adapter2 = builder.build("c1");
    Assert.assertEquals(adapter1, adapter2);
    Mockito.verify(propertyProvider, Mockito.times(1)).apply(Mockito.anyString());
  }

  @Test
  public void testListTopics() {
    AdminClient.listTopics();
    Mockito.verify(delegator, Mockito.times(1)).listTopics();
  }

  @Test
  public void testDescribeTopics() {
    AdminClient.describeTopics(Collections.singletonList("topic1"));
    ArgumentCaptor<Collection> captor = ArgumentCaptor.forClass(Collection.class);
    Mockito.verify(delegator, Mockito.times(1))
        .describeTopics(captor.capture(), Mockito.any(DescribeTopicsOptions.class));
    Assert.assertTrue(captor.getValue().contains("topic1"));
  }

  @Test
  public void testOffsetsForTimes() {
    TopicPartition tp = new TopicPartition("topic1", 0);
    AdminClient.offsetsForTimes(Collections.singletonMap(tp, 100L));
    ArgumentCaptor<Map<TopicPartition, OffsetSpec>> captor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(delegator, Mockito.times(1)).listOffsets(captor.capture());
    Assert.assertTrue(captor.getValue().get(tp) instanceof OffsetSpec.TimestampSpec);
  }

  @Test
  public void testBeginningOffsets() {
    TopicPartition tp = new TopicPartition("topic1", 0);
    AdminClient.beginningOffsets(Collections.singletonList(tp));
    ArgumentCaptor<Map<TopicPartition, OffsetSpec>> captor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(delegator, Mockito.times(1)).listOffsets(captor.capture());
    Assert.assertTrue(captor.getValue().get(tp) instanceof OffsetSpec.EarliestSpec);
  }

  @Test
  public void testEndOffsets() {
    TopicPartition tp = new TopicPartition("topic1", 0);
    AdminClient.endOffsets(Collections.singletonList(tp));
    ArgumentCaptor<Map<TopicPartition, OffsetSpec>> captor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(delegator, Mockito.times(1)).listOffsets(captor.capture());
    Assert.assertTrue(captor.getValue().get(tp) instanceof OffsetSpec.LatestSpec);
  }

  @Test
  public void testListConsumerGroupOffsets() {
    AdminClient.listConsumerGroupOffsets("group1");
    Mockito.verify(delegator, Mockito.times(1)).listConsumerGroupOffsets("group1");
  }
}
