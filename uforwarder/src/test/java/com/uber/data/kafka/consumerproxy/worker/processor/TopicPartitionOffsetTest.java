package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TopicPartitionOffsetTest extends FievelTestBase {
  private String topic = "topic";
  private int partition = 1;
  private long offset = 2;
  private TopicPartitionOffset topicPartitionOffset;
  private TopicPartitionOffset topicPartitionOffset1;
  private TopicPartitionOffset topicPartitionOffset2;
  private TopicPartitionOffset topicPartitionOffset3;
  private TopicPartitionOffset topicPartitionOffset4;

  @Before
  public void setUp() {
    topicPartitionOffset = new TopicPartitionOffset(topic, partition, offset);
    topicPartitionOffset1 = new TopicPartitionOffset(topic, partition, offset);
    topicPartitionOffset2 = new TopicPartitionOffset("another_topic", partition, offset);
    topicPartitionOffset3 = new TopicPartitionOffset(topic, 3, offset);
    topicPartitionOffset4 = new TopicPartitionOffset(topic, partition, 4);
  }

  @Test
  public void testGetters() {
    Assert.assertEquals(topic, topicPartitionOffset.getTopic());
    Assert.assertEquals(partition, topicPartitionOffset.getPartition());
    Assert.assertEquals(offset, topicPartitionOffset.getOffset());
  }

  @Test
  public void testEquals() {
    Assert.assertEquals(topicPartitionOffset, topicPartitionOffset);
    Assert.assertNotEquals(topicPartitionOffset, null);
    Assert.assertNotEquals(topicPartitionOffset, "");
    Assert.assertEquals(topicPartitionOffset, topicPartitionOffset1);
    Assert.assertNotEquals(topicPartitionOffset, topicPartitionOffset2);
    Assert.assertNotEquals(topicPartitionOffset, topicPartitionOffset3);
    Assert.assertNotEquals(topicPartitionOffset, topicPartitionOffset4);
  }

  @Test
  public void testHash() {
    Assert.assertEquals(topicPartitionOffset.hashCode(), topicPartitionOffset1.hashCode());
    Assert.assertNotEquals(topicPartitionOffset.hashCode(), topicPartitionOffset2.hashCode());
    Assert.assertNotEquals(topicPartitionOffset.hashCode(), topicPartitionOffset3.hashCode());
    Assert.assertNotEquals(topicPartitionOffset.hashCode(), topicPartitionOffset4.hashCode());
  }
}
