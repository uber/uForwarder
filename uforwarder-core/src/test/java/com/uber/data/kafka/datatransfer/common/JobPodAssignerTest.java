package com.uber.data.kafka.datatransfer.common;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Test;

public class JobPodAssignerTest extends FievelTestBase {

  @Test
  public void testNoopJobPodAssigner_withValidTopicPartitionInfo_returnsEmptyString() {
    Node leader = mock(Node.class);
    TopicPartitionInfo topicPartitionInfo = mock(TopicPartitionInfo.class);

    when(topicPartitionInfo.leader()).thenReturn(leader);

    String result = JobPodAssigner.NoopJobPodAssigner.assignJobPod(topicPartitionInfo);

    assertEquals("", result);
  }
}
