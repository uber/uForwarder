package com.uber.data.kafka.datatransfer.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

public class JobPodAssignerTest {

  @Test
  public void testNoopJobPodAssigner_withValidTopicPartitionInfo_returnsEmptyString() {
    Node leader = mock(Node.class);
    TopicPartitionInfo topicPartitionInfo = mock(TopicPartitionInfo.class);

    when(topicPartitionInfo.leader()).thenReturn(leader);

    String result = JobPodAssigner.NoopJobPodAssigner.assignJobPod(topicPartitionInfo);

    assertEquals("", result);
  }
}
