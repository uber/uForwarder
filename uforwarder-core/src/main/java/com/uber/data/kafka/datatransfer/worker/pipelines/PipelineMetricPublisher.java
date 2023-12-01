package com.uber.data.kafka.datatransfer.worker.pipelines;

import org.springframework.scheduling.annotation.Scheduled;

/** PipelineMetricPublisher publish pipeline metrics job.host - mapping from job to host */
public class PipelineMetricPublisher {

  private final PipelineManager pipelineManager;

  public PipelineMetricPublisher(PipelineManager pipelineManager) {
    this.pipelineManager = pipelineManager;
  }

  /** Publish job.host metric for all job assigned to the worker every 5s */
  @Scheduled(fixedRate = 5000)
  protected void publish() {
    pipelineManager.publishMetrics();
  }
}
