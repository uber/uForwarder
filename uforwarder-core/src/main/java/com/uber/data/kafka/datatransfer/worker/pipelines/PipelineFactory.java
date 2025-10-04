package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.Job;
import java.util.function.Supplier;

/** PipelineFactory is used by the DataTransferFramework PipelineManager to create Pipelines. */
public interface PipelineFactory {
  String DELIMITER = "__";
  Supplier<Double> NOOP_CPU_USAGE_SUPPLIER = () -> 0.0;

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

  /**
   * Composes thread name based on the serviceName, role(producer/consumer/processor) and job.
   *
   * @param job job
   * @param role producer/consumer/processor
   * @param serviceName serviceName
   * @return actor(thread) name
   */
  default String getThreadName(Job job, String role, String serviceName) {
    return String.join(DELIMITER, serviceName, getPipelineId(job), role);
  }
}
