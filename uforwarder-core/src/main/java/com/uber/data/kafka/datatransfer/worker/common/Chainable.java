package com.uber.data.kafka.datatransfer.worker.common;

/**
 * Chainable is a common interface used by the DataTransferWorker framework for classes can use to
 * register a follow up stage.
 */
public interface Chainable<OUT, RESPONSE> {
  default void setNextStage(Sink<OUT, RESPONSE> sink) {}
}
