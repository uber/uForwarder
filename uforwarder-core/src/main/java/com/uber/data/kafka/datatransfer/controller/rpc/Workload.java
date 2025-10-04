package com.uber.data.kafka.datatransfer.controller.rpc;

import com.uber.data.kafka.datatransfer.controller.autoscalar.Throughput;

/**
 * Represents the workload metrics for a data processing pipeline.
 *
 * <p>This class extends {@link Throughput} to include CPU usage metrics alongside throughput
 * measurements. It provides a comprehensive view of a pipeline's resource consumption by combining
 * message throughput, data throughput, and CPU utilization.
 *
 * <p>The workload metrics include:
 *
 * <ul>
 *   <li>Messages per second (from {@link Throughput})
 *   <li>Bytes per second (from {@link Throughput})
 *   <li>CPU usage is the percentage of CPU time used by the workload in terms number of cores
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * // Create a workload with 1000 msg/s, 1MB/s, and 25% CPU usage
 * Workload workload = Workload.of(1000.0, 1_000_000.0, 0.25);
 *
 * // Access metrics
 * double msgRate = workload.getMessagesPerSecond();  // 1000.0
 * double byteRate = workload.getBytesPerSecond();    // 1_000_000.0
 * double cpuUsage = workload.getCpuUsage();         // 0.25
 * }</pre>
 *
 * <p>This class is immutable and thread-safe.
 *
 * @see Throughput
 * @since 1.0
 */
public class Workload extends Throughput {
  private final double cpuUsage;

  /**
   * Private constructor to create a new Workload instance.
   *
   * <p>Use {@link #of(double, double, double)} to create instances of this class.
   *
   * @param messagesPerSecond the number of messages processed per second
   * @param bytesPerSecond the number of bytes processed per second
   * @param cpuUsage the CPU usage as the number of cores used
   */
  private Workload(double messagesPerSecond, double bytesPerSecond, double cpuUsage) {
    super(messagesPerSecond, bytesPerSecond);
    this.cpuUsage = cpuUsage;
  }

  /**
   * Creates a new Workload instance with the specified metrics.
   *
   * <p>This factory method creates a new immutable Workload object that encapsulates throughput
   * metrics (messages and bytes per second) along with CPU usage information.
   *
   * @param messagesPerSecond the number of messages processed per second
   * @param bytesPerSecond the number of bytes processed per second
   * @param cpuUsage the CPU usage as the number of cores used
   * @return a new Workload instance with the specified metrics
   */
  public static Workload of(double messagesPerSecond, double bytesPerSecond, double cpuUsage) {
    return new Workload(messagesPerSecond, bytesPerSecond, cpuUsage);
  }

  /**
   * Gets the CPU usage value.
   *
   * <p>Returns the CPU usage where there number represents number of cores used
   *
   * <p>The CPU usage value represents the proportion of available CPU time being consumed by the
   * workload. For example, a value of 0.25 indicates that the workload is using 25% of 1 CPU core
   *
   * @return the CPU usage
   */
  public double getCpuUsage() {
    return cpuUsage;
  }
}
