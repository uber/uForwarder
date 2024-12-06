package com.uber.data.kafka.consumerproxy.worker.filter;

import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorMessage;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class OriginalClusterFilterTest extends FievelTestBase {
  private Filter originalClusterFilter;
  private Job job;
  private ProcessorMessage processorMessage;
  private String cluster1 = "cluster-1";

  @Before
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
    Assert.assertTrue(originalClusterFilter.shouldProcess(ItemAndJob.of(processorMessage, job)));
  }

  @Test
  public void testShouldProcessCluster1() {
    Mockito.when(processorMessage.getProducerCluster()).thenReturn(cluster1);
    Assert.assertTrue(originalClusterFilter.shouldProcess(ItemAndJob.of(processorMessage, job)));
  }

  @Test
  public void testShouldProcessCluster2() {
    Mockito.when(processorMessage.getProducerCluster()).thenReturn("cluster2");
    Assert.assertFalse(originalClusterFilter.shouldProcess(ItemAndJob.of(processorMessage, job)));
  }
}
