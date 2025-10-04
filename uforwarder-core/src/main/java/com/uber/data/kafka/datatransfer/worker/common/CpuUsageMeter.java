package com.uber.data.kafka.datatransfer.worker.common;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;

/**
 * A concrete implementation for measuring CPU usage.
 *
 * <p>This class provides CPU usage measurement functionality using a Meter-based approach to track
 * CPU time over time. It provides accurate CPU usage measurements for monitoring and resource
 * management purposes.
 *
 * <p>The class uses a configurable {@link Ticker} for time measurements, allowing for both system
 * time and test scenarios. By default, it uses the system ticker for production use.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * CpuUsageMeter meter = new CpuUsageMeter();
 * meter.mark(cpuTimeNanos); // Record CPU time measurement
 * double usage = meter.getUsage(); // Get current CPU usage percentage
 * }</pre>
 *
 * @since 1.0
 */
public class CpuUsageMeter {
  private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  private final Meter meter;

  /**
   * Constructs a new CpuUsageMeter using the system ticker.
   *
   * <p>This constructor creates a meter that uses the system's default time source for CPU usage
   * measurements.
   */
  public CpuUsageMeter() {
    this(Ticker.systemTicker());
  }

  /**
   * Constructs a new CpuUsageMeter with a custom ticker.
   *
   * <p>This constructor allows for dependency injection of a custom ticker, which is useful for
   * testing scenarios where controlled time measurements are needed.
   *
   * @param ticker the ticker to use for time measurements
   */
  public CpuUsageMeter(Ticker ticker) {
    meter = new Meter(new TickerClock(ticker));
  }

  /**
   * Records a CPU time measurement for usage tracking.
   *
   * <p>This method records CPU time in nanoseconds for the internal meter. The CPU time should
   * represent the actual CPU time consumed by the process or thread being monitored.
   *
   * <p>It is recommended to call this method at regular intervals (e.g., every second) to maintain
   * accurate CPU usage statistics. The frequency of calls determines the granularity of CPU usage
   * tracking.
   *
   * @param cpuTimeNanos the CPU time in nanoseconds to record
   */
  public void mark(long cpuTimeNanos) {
    meter.mark(cpuTimeNanos);
  }

  /**
   * Gets the current CPU usage as a percentage.
   *
   * <p>Returns the CPU usage percentage based on measurements recorded by previous calls to {@link
   * #mark(long)}. The returned value represents the average CPU usage over a one-minute window,
   * calculated as the rate of CPU time consumption per second. the result is irrelevant to the
   * number of cores in the machine.
   *
   * <p>If no measurements have been recorded (i.e., {@link #mark(long)} has never been called),
   * returns 0.0.
   *
   * @return the current CPU usage percentage as a double value
   */
  public double getUsage() {
    return meter.getOneMinuteRate() / NANOS_PER_SECOND;
  }

  /**
   * A custom Clock implementation that uses a Ticker for time measurements.
   *
   * <p>This inner class adapts the Guava Ticker interface to work with the Codahale Metrics Clock
   * interface, allowing the Meter to use the provided ticker for time-based calculations.
   */
  private static class TickerClock extends Clock {
    private final Ticker ticker;

    /**
     * Constructs a new TickerClock with the specified ticker.
     *
     * @param ticker the ticker to use for time measurements
     */
    TickerClock(Ticker ticker) {
      this.ticker = ticker;
    }

    @Override
    public long getTick() {
      return ticker.read();
    }
  }
}
