package com.uber.data.kafka.datatransfer.worker.common;

/**
 * Interface for measuring CPU usage.
 *
 * <p>This interface provides methods to track CPU usage over time. Implementations should provide
 * accurate CPU usage measurements for monitoring and resource management purposes.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * CpuUsageMeter meter = new SomeCpuUsageMeter();
 * meter.tick(); // Record a measurement point
 * double usage = meter.getUsage(); // Get current CPU usage percentage
 * }</pre>
 *
 * @since 1.0
 */
public interface CpuUsageMeter {
  CpuUsageMeter NOOP =
      new CpuUsageMeter() {
        @Override
        public void tick() {}

        @Override
        public double getUsage() {
          return 0;
        }
      };

  /**
   * Records a measurement point for CPU usage tracking.
   *
   * <p>This method should be called periodically to record CPU usage measurements. The frequency of
   * calls determines the granularity of CPU usage tracking. Implementations typically capture
   * system CPU metrics at the time of calling.
   *
   * <p>It is recommended to call this method at regular intervals (e.g., every second) to maintain
   * accurate CPU usage statistics.
   */
  void tick();

  /**
   * Gets the current CPU usage as a percentage.
   *
   * <p>Returns the CPU usage percentage based on measurements recorded by previous calls to {@link
   * #tick()}. The returned value represents the average CPU usage over the measurement period.
   *
   * <p>If no measurements have been recorded (i.e., {@link #tick()} has never been called), returns
   * 0.0.
   *
   * @return the current CPU usage percentage as a double value
   */
  double getUsage();
}
