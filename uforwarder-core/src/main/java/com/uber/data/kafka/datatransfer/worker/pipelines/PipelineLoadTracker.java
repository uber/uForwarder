package com.uber.data.kafka.datatransfer.worker.pipelines;

/**
 * Interface for tracking pipeline load metrics.
 *
 * <p>This interface defines a contract for classes that can track and provide load information for
 * data processing pipelines. It provides methods to retrieve current load metrics and to properly
 * close the tracker when it's no longer needed.
 *
 * <p>The load tracking includes both core thread usage (specific to the pipeline) and system usage
 * (portion of system-wide resources used by this pipeline). This allows for comprehensive
 * monitoring of resource consumption across multiple pipelines.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * PipelineLoadTracker tracker = pipelineLoadManager.createTracker("my-pipeline");
 *
 * // Get current load metrics
 * Load load = tracker.getLoad();
 * double coreUsage = load.getCoreThreadUsage();
 * double systemUsage = load.getSystemUsage();
 *
 * // Close when done
 * boolean closed = tracker.close();
 * }</pre>
 *
 * <p>Implementations should ensure thread safety if they are intended to be used in multi-threaded
 * environments.
 *
 * @since 1.0
 */
public interface PipelineLoadTracker {

  /**
   * A no-operation implementation that always returns zero load.
   *
   * <p>This constant provides a safe default implementation that can be used when load tracking is
   * not required or when the actual tracker is not available. It always returns zero values for
   * both core thread usage and system usage.
   */
  PipelineLoadTracker NOOP =
      new PipelineLoadTracker() {
        @Override
        public PipelineLoad getLoad() {
          return PipelineLoad.ZERO;
        }

        @Override
        public boolean close() {
          return false;
        }
      };

  /**
   * Gets the current load metrics for the pipeline.
   *
   * <p>Returns a {@link PipelineLoad} object containing the current load measurements for the
   * pipeline. The load includes both core thread usage (CPU time consumed by the pipeline's
   * threads) and system usage (portion of system-wide resources used by this pipeline).
   *
   * <p>The load values are typically calculated based on recent measurements and may be averaged
   * over a time window. The specific calculation method depends on the implementation.
   *
   * @return the current load metrics for the pipeline
   */
  PipelineLoad getLoad();

  /**
   * Closes the tracker and releases associated resources.
   *
   * <p>This method should be called when the tracker is no longer needed to ensure proper cleanup
   * of resources. The method returns {@code true} if the tracker was successfully closed, or {@code
   * false} if it was already closed or could not be closed.
   *
   * <p>After calling this method, the tracker should not be used for further operations. Attempting
   * to call {@link #getLoad()} after closing may result in undefined behavior.
   *
   * @return {@code true} if the tracker was successfully closed, {@code false} otherwise
   */
  boolean close();

  /** Represents load metrics for a pipeline. */
  interface PipelineLoad {
    PipelineLoad ZERO =
        new PipelineLoad() {
          @Override
          public double getCoreCpuUsage() {
            return 0.0;
          }

          @Override
          public double getCpuUsage() {
            return 0.0;
          }
        };

    /**
     * Gets the core thread CPU usage.
     *
     * <p>This value represents the CPU time consumed by threads specifically associated with this
     * pipeline. The value is typically normalized and represents the proportion of CPU time used
     * relative to the total available CPU time.
     *
     * <p>For example, a value of 0.25 indicates that the pipeline's threads are consuming 25% of
     * one CPU core's time, regardless of the total number of cores in the system.
     *
     * @return the core thread usage value
     */
    double getCoreCpuUsage();

    /**
     * Gets the computed cpu usage of the pipeline.
     *
     * <p>This value represents the portion of system-wide resources used by this pipeline. The
     * computation of this value can vary depending on the implementation. A typical approach is to
     * use the core thread usage to project the system usage, but different implementations may use
     * alternative calculation methods.
     *
     * <p>For example, a value of 0.1 indicates that this pipeline is using 10% of the system-wide
     * resources based on the implementation's calculation method.
     *
     * @return the system usage value
     */
    double getCpuUsage();
  }
}
