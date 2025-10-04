package com.uber.data.kafka.consumerproxy.worker.filter;

import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorMessage;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class OriginalClusterFilterTest {
  private Filter originalClusterFilter;
  private Job job;
  private ProcessorMessage processorMessage;
  private String cluster1 = "cluster-1";

  @BeforeEach
  public void setup() {
    job =
        Job.newBuilder()
            .setKafkaConsumerTask(KafkaConsumerTask.newBuilder().setCluster(cluster1).build())
            .build();
    Filter.Factory factory = OriginalClusterFilter.newFactory();
    processorMessage = Mockito.mock(ProcessorMessage.class);
    originalClusterFilter = factory.create(null);
  }

  @Test
  public void testShouldProcessEmptyCluster() {
    Mockito.when(processorMessage.getProducerCluster()).thenReturn("");
    Assertions.assertTrue(
        originalClusterFilter.shouldProcess(ItemAndJob.of(processorMessage, job)));
  }

  @Test
  public void testShouldProcessCluster1() {
    Mockito.when(processorMessage.getProducerCluster()).thenReturn(cluster1);
    Assertions.assertTrue(
        originalClusterFilter.shouldProcess(ItemAndJob.of(processorMessage, job)));
  }

  @Test
  public void testShouldProcessCluster2() {
    Mockito.when(processorMessage.getProducerCluster()).thenReturn("cluster2");
    Assertions.assertFalse(
        originalClusterFilter.shouldProcess(ItemAndJob.of(processorMessage, job)));
  }
}
