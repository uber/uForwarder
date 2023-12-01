package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PipelineMetricPublisherTest extends FievelTestBase {
  private PipelineMetricPublisher metricPublisher;
  private PipelineManager pipelineManager;

  @Before
  public void setup() {
    pipelineManager = Mockito.mock(PipelineManager.class);
    metricPublisher = new PipelineMetricPublisher(pipelineManager);
  }

  @Test
  public void testPublish() {
    metricPublisher.publish();
    Mockito.verify(pipelineManager, Mockito.times(1)).publishMetrics();
  }
}
