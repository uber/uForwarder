package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.Job;

/** PipelineFactory is used by the DataTransferFramework PipelineManager to create Pipelines. */
public interface PipelineFactory {

  /**
   * Create a new Pipeline.
   *
   * @param job to create pipeline for.
   * @return created Pipeline
   * @implNote the framework guarantees that createPipeline is called exactly once for each
   *     pipelineId.
   */
  Pipeline createPipeline(String pipelineId, Job job);

  /**
   * Get the pipeline id for a job. This method allows custom grouping of jobs for to a pipeline.
   *
   * @param job that we are querying for.
   * @return String that will be used as the pipeline id for this job.
   * @implSpec pipelineId must be consistent for a fixed jobDefinition.
   */
  String getPipelineId(Job job);
}
