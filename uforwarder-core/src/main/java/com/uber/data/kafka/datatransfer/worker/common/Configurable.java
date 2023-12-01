package com.uber.data.kafka.datatransfer.worker.common;

/**
 * Configurable is a common interface used by the DataTransferWorker framework to register a
 * configuration manager.
 */
public interface Configurable {
  default void setPipelineStateManager(PipelineStateManager pipelineStateManager) {}
}
