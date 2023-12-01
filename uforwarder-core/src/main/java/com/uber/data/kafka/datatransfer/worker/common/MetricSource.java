package com.uber.data.kafka.datatransfer.worker.common;

/**
 * MetricSource publishes metrics in passive way
 *
 * <pre>
 *   Proactive metrics publish can be blocked, and cause metrics missing
 *   Passive metrics is published by dedicated thread, and can't be blocked
 * </pre>
 */
public interface MetricSource {

  /**
   * Publish metrics.
   *
   * <pre>
   *   This function should not be blocked
   * </pre>
   */
  default void publishMetrics() {}
}
