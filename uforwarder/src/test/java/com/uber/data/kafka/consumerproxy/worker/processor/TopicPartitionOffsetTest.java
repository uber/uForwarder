package com.uber.data.kafka.consumerproxy.worker.processor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TopicPartitionOffsetTest {
  private String topic = "topic";
  private int partition = 1;
  private long offset = 2;
  private TopicPartitionOffset topicPartitionOffset;
  private TopicPartitionOffset topicPartitionOffset1;
  private TopicPartitionOffset topicPartitionOffset2;
  private TopicPartitionOffset topicPartitionOffset3;
  private TopicPartitionOffset topicPartitionOffset4;

  @BeforeEach
  public void setUp() {
    topicPartitionOffset = new TopicPartitionOffset(topic, partition, offset);
    topicPartitionOffset1 = new TopicPartitionOffset(topic, partition, offset);
    topicPartitionOffset2 = new TopicPartitionOffset("another_topic", partition, offset);
    topicPartitionOffset3 = new TopicPartitionOffset(topic, 3, offset);
    topicPartitionOffset4 = new TopicPartitionOffset(topic, partition, 4);
  }

  @Test
  public void testGetters() {
    Assertions.assertEquals(topic, topicPartitionOffset.getTopic());
    Assertions.assertEquals(partition, topicPartitionOffset.getPartition());
    Assertions.assertEquals(offset, topicPartitionOffset.getOffset());
  }

  @Test
  public void testEquals() {
    Assertions.assertEquals(topicPartitionOffset, topicPartitionOffset);
    Assertions.assertNotEquals(topicPartitionOffset, null);
    Assertions.assertNotEquals(topicPartitionOffset, "");
    Assertions.assertEquals(topicPartitionOffset, topicPartitionOffset1);
    Assertions.assertNotEquals(topicPartitionOffset, topicPartitionOffset2);
    Assertions.assertNotEquals(topicPartitionOffset, topicPartitionOffset3);
    Assertions.assertNotEquals(topicPartitionOffset, topicPartitionOffset4);
  }

  @Test
  public void testHash() {
    Assertions.assertEquals(topicPartitionOffset.hashCode(), topicPartitionOffset1.hashCode());
    Assertions.assertNotEquals(topicPartitionOffset.hashCode(), topicPartitionOffset2.hashCode());
    Assertions.assertNotEquals(topicPartitionOffset.hashCode(), topicPartitionOffset3.hashCode());
    Assertions.assertNotEquals(topicPartitionOffset.hashCode(), topicPartitionOffset4.hashCode());
  }
}
