package com.uber.data.kafka.datatransfer.worker.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * A thread registry that tracks threads and provides CPU usage measurement capabilities.
 *
 * <p>This class provide CPU usage monitoring for registered threads. It maintains a weak reference
 * set of threads to allow garbage collection of terminated threads, and provides a thread factory
 * that automatically registers created threads.
 *
 * <p>The CPU usage measurement is only available when thread CPU time is enabled in the JVM. When
 * disabled, the meter will return {@link Double#NaN} to indicate that CPU time measurement is not
 * available.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Thread tracking with automatic garbage collection support
 *   <li>CPU usage monitoring and measurement
 *   <li>Thread factory integration for automatic registration
 *   <li>Thread-safe operations using concurrent collections
 *   <li>Automatic CPU time measurement on each usage query
 * </ul>
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
 * ThreadRegister register = new ThreadRegister(threadMXBean);
 *
 * // Use the thread factory to create monitored threads
 * ThreadFactory monitoredFactory = register.asThreadFactory();
 * ExecutorService executor = Executors.newFixedThreadPool(10, monitoredFactory);
 *
 * // Get CPU usage - automatically measures and returns current usage
 * double cpuUsage = register.getUsage();
 * }</pre>
 *
 * <p>This class is thread-safe and can be used in multi-threaded environments. The CPU usage
 * measurement is independent of the number of CPU cores in the machine.
 */
public class ThreadRegister {
  /** The default thread factory used for creating new threads. */
  private static final ThreadFactory BACKING_THREAD_FACTORY = Executors.defaultThreadFactory();

  private final CpuUsageMeter cpuUsageMeter;
  private final ThreadMXBean threadMXBean;

  /**
   * Set of registered threads, using WeakHashMap to allow garbage collection of terminated threads.
   */
  private final Set<Thread> threads;

  private final ConcurrentMap<Thread, Long> threadToLastCpuTime;
  private final boolean enabled;

  /**
   * Constructs a new ThreadRegister instance.
   *
   * <p>Creates a new weak reference set for tracking registered threads and initializes CPU usage
   * measurement capabilities. The CPU time measurement will only be enabled if the JVM supports
   * thread CPU time tracking.
   *
   * @param threadMXBean the ThreadMXBean to use for CPU time measurements
   */
  public ThreadRegister(ThreadMXBean threadMXBean) {
    this(threadMXBean, new CpuUsageMeter());
  }

  /**
   * Constructs a new ThreadRegister instance with a custom CPU usage meter.
   *
   * <p>This constructor is primarily used for testing purposes to inject a custom CPU usage meter.
   *
   * @param threadMXBean the ThreadMXBean to use for CPU time measurements
   * @param cpuUsageMeter the custom CPU usage meter to use for measurements
   */
  @VisibleForTesting
  ThreadRegister(ThreadMXBean threadMXBean, CpuUsageMeter cpuUsageMeter) {
    this.threads = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
    this.cpuUsageMeter = cpuUsageMeter;
    this.threadMXBean = threadMXBean;
    this.threadToLastCpuTime = new ConcurrentHashMap<>();
    this.enabled = threadMXBean.isThreadCpuTimeEnabled();
  }

  /**
   * Returns a thread factory that automatically registers created threads.
   *
   * <p>The returned factory creates new threads that are automatically registered with the
   * ThreadRegister instance. This allows for easy monitoring and management of thread resources.
   * Any threads created through this factory will be tracked for CPU usage measurement.
   *
   * @return a thread factory that automatically registers created threads
   */
  public ThreadFactory asThreadFactory() {
    return r -> {
      Thread thread = BACKING_THREAD_FACTORY.newThread(r);
      register(thread);
      return thread;
    };
  }

  /**
   * Registers a thread with this ThreadRegister instance.
   *
   * <p>Adds the specified thread to the internal thread set for monitoring and tracking. The thread
   * will be tracked using weak references, allowing it to be garbage collected when it terminates
   * and is no longer referenced elsewhere.
   *
   * <p>This method is typically called automatically when using the thread factory returned by
   * {@link #asThreadFactory()}, but can also be called manually to register existing threads.
   *
   * @param thread the thread to register, must not be null
   * @param <T> the type of the thread
   * @return the registered thread for method chaining
   * @throws NullPointerException if thread is null
   */
  public <T extends Thread> T register(T thread) {
    threads.add(thread);
    return thread;
  }

  /**
   * Returns a copy of the currently registered threads.
   *
   * <p>This method is primarily used for testing purposes to verify thread registration.
   *
   * @return an immutable collection of currently registered threads
   */
  @VisibleForTesting
  protected Collection<Thread> getRegistered() {
    return ImmutableSet.copyOf(this.threads);
  }

  /**
   * Gets the current CPU usage as a percentage.
   *
   * <p>This method automatically measures CPU usage for all registered threads and returns the
   * current usage percentage. The measurement is performed by calculating the difference in CPU
   * time for each registered thread since the last measurement.
   *
   * <p>If thread CPU time is not enabled in the JVM, this method returns {@link Double#NaN}.
   * Otherwise, it returns a value between 0.0 and 1.0 representing the CPU usage percentage, where
   * the measurement is independent of the number of CPU cores in the machine.
   *
   * @return the current CPU usage percentage as a double value in (0.0, 1.0], or 0.0 if thread CPU
   *     time is not enabled
   */
  public double getUsage() {
    if (!enabled) {
      return 0.0;
    }

    tick();

    return cpuUsageMeter.getUsage();
  }

  /**
   * Performs CPU time measurement for all registered threads.
   *
   * <p>This method iterates through all registered threads, calculates the CPU time consumed by
   * each thread since the last measurement, and records the total CPU time in the internal meter.
   * The measurement is only performed if thread CPU time is enabled in the JVM.
   */
  private void tick() {
    if (!enabled) {
      return;
    }

    long localCpuTime = 0L;
    for (Thread thread : threads) {
      long currentThreadCpuTime = threadMXBean.getThreadCpuTime(thread.getId());
      Long lastThreadTime = threadToLastCpuTime.put(thread, currentThreadCpuTime);
      if (lastThreadTime != null) {
        localCpuTime += currentThreadCpuTime - lastThreadTime;
      }
    }
    cpuUsageMeter.mark(localCpuTime);
  }
}
