package com.uber.data.kafka.datatransfer.worker.pipelines;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class PipelineMetricPublisherTest {
  private PipelineMetricPublisher metricPublisher;
  private PipelineManager pipelineManager;

  @BeforeEach
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
