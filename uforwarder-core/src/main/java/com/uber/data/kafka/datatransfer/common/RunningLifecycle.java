package com.uber.data.kafka.datatransfer.common;

import org.springframework.context.Lifecycle;

/**
 * RunningLifecycle is a default implementation of Spring Lifecycle for an object that is always in
 * a running state
 */
public interface RunningLifecycle extends Lifecycle {
  /**
   * Start the object lifecycle.
   *
   * <p>Default: noop.
   */
  @Override
  default void start() {}

  /**
   * Check whether the object is running.
   *
   * @return true if object lifecycle is running.
   *     <p>Default: true.
   */
  @Override
  default boolean isRunning() {
    return true;
  }

  /**
   * Stop object lifecycle.
   *
   * <p>Default: noop.
   */
  @Override
  default void stop() {}
}
