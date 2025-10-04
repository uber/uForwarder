package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.sun.management.OperatingSystemMXBean;
import com.uber.concurrency.loadbalancer.utils.IntervalLimiter;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.CpuUsageMeter;
import com.uber.data.kafka.datatransfer.worker.common.ThreadRegister;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages load tracking for multiple data processing pipelines.
 *
 * <p>This class provides centralized management of load tracking for data processing pipelines. It
 * creates and manages {@link LoadTracker} instances for individual pipelines, computes resource
 * shares among pipelines, and provides thread-safe access to load metrics.
 *
 * <p>The manager uses an interval-based approach to update pipeline shares, ensuring that resource
 * allocation calculations are performed at regular intervals rather than on every load request.
 * This improves performance while maintaining accurate load distribution information.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Thread-safe tracker creation and management
 *   <li>Automatic share computation based on CPU usage
 *   <li>Interval-based share updates for performance
 *   <li>Integration with system load metrics
 *   <li>Automatic cleanup of closed trackers
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * CoreInfra coreInfra = // ... obtain core infrastructure
 * PipelineLoadManager manager = new PipelineLoadManager(coreInfra);
 *
 * // Create trackers for different pipelines
 * LoadTracker tracker1 = manager.createTracker("pipeline-1");
 * LoadTracker tracker2 = manager.createTracker("pipeline-2");
 *
 * // Get load metrics
 * Load load1 = tracker1.getLoad();
 * Load load2 = tracker2.getLoad();
 *
 * // Close trackers when done
 * tracker1.close();
 * tracker2.close();
 * }</pre>
 *
 * <p>This class is thread-safe and can be used in multi-threaded environments.
 *
 * @since 1.0
 */
public class PipelineLoadManager {

  /**
   * The interval between share updates in nanoseconds.
   *
   * <p>Shares are recomputed every 10 seconds to balance accuracy with performance. This prevents
   * excessive computation while ensuring load distribution remains current.
   */
  private static final long SHARE_UPDATE_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

  private final CpuUsageMeter processCpuUsageMeter;
  private final CoreInfra coreInfra;
  private final ConcurrentHashMap<String, LoadTracker> idToTracker;
  private final IntervalLimiter shareUpdateIntervalLimiter;
  private final AtomicLong processCpuTimeNanos = new AtomicLong(0);
  private final Optional<OperatingSystemMXBean> operatingSystemMXBean;

  /**
   * Constructs a new PipelineLoadManager with the specified core infrastructure.
   *
   * <p>This constructor uses the system ticker for time-based operations. It initializes the
   * interval limiter for share updates and prepares the manager for tracker creation.
   *
   * @param coreInfra the core infrastructure providing system metrics and thread management
   */
  public PipelineLoadManager(CoreInfra coreInfra) {
    this(coreInfra, Ticker.systemTicker());
  }

  /**
   * Constructs a new PipelineLoadManager with the specified core infrastructure and ticker.
   *
   * <p>This constructor allows for dependency injection of a custom ticker, which is useful for
   * testing scenarios where controlled time measurements are needed.
   *
   * @param coreInfra the core infrastructure providing system metrics and thread management
   * @param ticker the ticker to use for time-based operations
   */
  protected PipelineLoadManager(CoreInfra coreInfra, Ticker ticker) {
    this.coreInfra = coreInfra;
    this.idToTracker = new ConcurrentHashMap<>();
    this.shareUpdateIntervalLimiter = new IntervalLimiter(SHARE_UPDATE_INTERVAL_NANOS, ticker);
    this.processCpuUsageMeter = new CpuUsageMeter(ticker);
    if (coreInfra.getOperatingSystemMXBean() instanceof OperatingSystemMXBean) {
      operatingSystemMXBean =
          Optional.of((OperatingSystemMXBean) coreInfra.getOperatingSystemMXBean());
    } else {
      operatingSystemMXBean = Optional.empty();
    }
  }

  /**
   * Creates or retrieves a load tracker for the specified pipeline.
   *
   * <p>This method provides thread-safe access to load trackers. If a tracker for the given
   * pipeline ID already exists, it returns the existing instance. Otherwise, it creates a new
   * tracker and stores it for future use.
   *
   * <p>The tracker is automatically associated with the pipeline ID and will be used for share
   * computation and load measurement.
   *
   * @param pipelineId the unique identifier for the pipeline
   * @return the load tracker for the specified pipeline
   */
  public LoadTracker createTracker(String pipelineId) {
    return idToTracker.computeIfAbsent(pipelineId, LoadTracker::new);
  }

  /**
   * Triggers share computation if the update interval has elapsed.
   *
   * <p>This method checks if enough time has passed since the last share computation. If so, it
   * triggers a new computation to update the resource shares among all active pipelines.
   */
  private void tick() {
    markCpuTime();

    if (shareUpdateIntervalLimiter.acquire() > 0) {
      // update shares in PipelineState
      computeShare();
    }
  }

  private void markCpuTime() {
    if (operatingSystemMXBean.isPresent()) {
      final long lastProcessCpuTimeNanos = processCpuTimeNanos.get();
      final long newProcessCpuTimeNanos = operatingSystemMXBean.get().getProcessCpuTime();
      if (processCpuTimeNanos.compareAndSet(lastProcessCpuTimeNanos, newProcessCpuTimeNanos)) {
        processCpuUsageMeter.mark(newProcessCpuTimeNanos - lastProcessCpuTimeNanos);
      }
    }
  }

  /**
   * Computes resource shares for all active pipelines.
   *
   * <p>This method calculates the relative resource allocation for each pipeline based on their
   * core thread CPU usage. The share is computed as the ratio of a pipeline's CPU usage to the
   * total CPU usage across all pipelines.
   *
   * <p>If the total usage is zero (no CPU activity), all pipelines receive a share of zero.
   * Otherwise, each pipeline's share is proportional to its CPU usage.
   */
  private void computeShare() {
    List<LoadTracker> trackers = new ArrayList<>(idToTracker.values());
    double[] usages = new double[trackers.size()];
    double totalUsage = 0.0;
    for (int i = 0; i < trackers.size(); i++) {
      LoadTracker loadState = trackers.get(i);
      usages[i] = loadState.threadRegister.getUsage();
      totalUsage += usages[i];
    }
    for (int i = 0; i < trackers.size(); i++) {
      LoadTracker pipelineState = trackers.get(i);
      if (totalUsage == 0.0) {
        pipelineState.share = 0.0;
      } else {
        pipelineState.share = usages[i] / totalUsage;
      }
    }
  }

  /**
   * A load tracker for an individual pipeline.
   *
   * <p>This inner class implements {@link PipelineLoadTracker} and provides load tracking
   * capabilities for a specific pipeline. It maintains thread registration, computes load metrics,
   * and manages its lifecycle.
   *
   * <p>Each tracker is associated with a unique pipeline ID and provides:
   *
   * <ul>
   *   <li>Thread registration for CPU usage tracking
   *   <li>Load computation including core thread usage and system usage
   *   <li>Resource share management
   *   <li>Lifecycle management with proper cleanup
   * </ul>
   *
   * <p>Usage example:
   *
   * <pre>{@code
   * LoadTracker tracker = manager.createTracker("my-pipeline");
   *
   * // Get current load
   * Load load = tracker.getLoad();
   * double coreUsage = load.getCoreThreadUsage();
   * double systemUsage = load.getSystemUsage();
   *
   * // Access thread register for custom monitoring
   * ThreadRegister register = tracker.getThreadRegister();
   *
   * // Close when done
   * boolean closed = tracker.close();
   * }</pre>
   *
   * <p>This class is thread-safe and can be used in multi-threaded environments.
   */
  public class LoadTracker implements PipelineLoadTracker {
    private final String pipelineId;
    private final AtomicBoolean isClosed;
    private volatile double share;
    private final ThreadRegister threadRegister;

    /**
     * Constructs a new LoadTracker for the specified pipeline.
     *
     * <p>This constructor initializes the tracker with the given pipeline ID, creates a thread
     * register for CPU usage tracking, and sets the initial share to zero.
     *
     * @param pipelineId the unique identifier for the pipeline
     */
    LoadTracker(String pipelineId) {
      this.isClosed = new AtomicBoolean(false);
      this.pipelineId = pipelineId;
      this.threadRegister = new ThreadRegister(coreInfra.getThreadMXBean());
      this.share = 0.0d;
    }

    /**
     * Gets the thread register associated with this tracker.
     *
     * <p>The thread register provides access to CPU usage tracking for threads associated with this
     * pipeline. It can be used for custom monitoring or additional metrics.
     *
     * @return the thread register for this pipeline
     */
    public ThreadRegister getThreadRegister() {
      return threadRegister;
    }

    /**
     * Gets the current load metrics for this pipeline.
     *
     * <p>This method computes and returns the current load for the pipeline. If the tracker has
     * been closed, it returns zero load. Otherwise, it triggers share computation if needed and
     * returns the current load metrics.
     *
     * <p>The load includes:
     *
     * <ul>
     *   <li>Core thread usage: CPU time consumed by the pipeline's threads
     *   <li>System usage: portion of system load allocated to this pipeline based on its share
     * </ul>
     *
     * @return the current load metrics for this pipeline
     */
    @Override
    public PipelineLoadTracker.PipelineLoad getLoad() {
      if (isClosed.get()) {
        return PipelineLoad.ZERO;
      }

      tick();

      return new Load(threadRegister.getUsage(), processCpuUsageMeter.getUsage() * share);
    }

    /**
     * Closes this tracker and removes it from the manager.
     *
     * <p>This method atomically closes the tracker and removes it from the manager's tracking map.
     * After closing, the tracker will return zero load for all subsequent calls to {@link
     * #getLoad()}.
     *
     * <p>The method returns {@code true} if the tracker was successfully closed, or {@code false}
     * if it was already closed.
     *
     * @return {@code true} if the tracker was successfully closed, {@code false} otherwise
     */
    @Override
    public boolean close() {
      if (isClosed.compareAndSet(false, true)) {
        idToTracker.remove(pipelineId);
        return true;
      }
      return false;
    }

    /**
     * Gets the pipeline ID associated with this tracker.
     *
     * <p>This method is primarily used for testing purposes to verify the pipeline ID of a tracker.
     *
     * @return the pipeline ID for this tracker
     */
    @VisibleForTesting
    protected String pipelineId() {
      return pipelineId;
    }
  }

  /**
   * Represents load metrics for a pipeline.
   *
   * <p>This class encapsulates the load information for a pipeline, including both core thread
   * usage and system usage. The values represent resource consumption as percentages or normalized
   * values.
   *
   * <p>Core thread usage represents the CPU time consumed by threads specifically associated with
   * this pipeline. System usage represents the portion of system-wide resources used by this
   * pipeline, which can be computed in various ways depending on the implementation.
   *
   * <p>Usage example:
   *
   * <pre>{@code
   * Load load = tracker.getLoad();
   * double coreUsage = load.getCoreThreadUsage(); // e.g., 0.25 (25% of one core)
   * double systemUsage = load.getSystemUsage();   // e.g., 0.1 (10% of system resources)
   * }</pre>
   *
   * <p>This class is immutable and thread-safe.
   */
  private static class Load implements PipelineLoadTracker.PipelineLoad {
    // coreCpuUsage: the CPU time consumed by threads specifically associated with this pipeline.
    // this value is measured on threads
    private final double coreCpuUsage;
    // cpuUsage: the portion of system-wide resources used by this pipeline.
    // A typical way to compute systemUsage is to use core thread usage to project the system usage
    // however, there could be different implementations
    // this value is computed based on coreCpuUsage and processCpuUsage
    private final double cpuUsage;

    /**
     * Constructs a new Load instance with the specified usage values.
     *
     * @param coreCpuUsage the core thread usage value (typically between 0.0 and 1.0)
     * @param cpuUsage the system usage value (typically between 0.0 and 1.0)
     */
    Load(double coreCpuUsage, double cpuUsage) {
      this.coreCpuUsage = coreCpuUsage;
      this.cpuUsage = cpuUsage;
    }

    /**
     * Gets the core thread usage value.
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
    public double getCoreCpuUsage() {
      return coreCpuUsage;
    }

    /**
     * Gets the system usage value.
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
    public double getCpuUsage() {
      return cpuUsage;
    }
  }
}
