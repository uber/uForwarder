package com.uber.data.kafka.datatransfer.worker.common;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
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
import java.util.concurrent.TimeUnit;

/**
 * A thread registry that tracks threads and provides CPU usage measurement capabilities.
 *
 * <p>This class implements {@link CpuUsageMeter} to provide CPU usage monitoring for registered
 * threads. It maintains a weak reference set of threads to allow garbage collection of terminated
 * threads, and provides a thread factory that automatically registers created threads.
 *
 * <p>The CPU usage measurement is only available when thread CPU time is enabled in the JVM. When
 * disabled, the meter will not record any CPU usage data.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Thread tracking with automatic garbage collection support
 *   <li>CPU usage monitoring and measurement
 *   <li>Thread factory integration for automatic registration
 *   <li>Thread-safe operations using concurrent collections
 *   <li>Configurable ticker for time-based measurements
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
 * // Periodically call tick() to update CPU usage measurements
 * register.tick();
 * double cpuUsage = register.getUsage();
 * }</pre>
 *
 * <p>This class is thread-safe and can be used in multi-threaded environments.
 */
public class ThreadRegister implements CpuUsageMeter {
  /** The default thread factory used for creating new threads. */
  private static final ThreadFactory BACKING_THREAD_FACTORY = Executors.defaultThreadFactory();

  private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  private final Meter cpuUsageMeter;
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
   * <p>Creates a new weak reference set for tracking registered threads.
   */
  public ThreadRegister(ThreadMXBean threadMXBean, Ticker ticker) {
    this.threads = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
    this.cpuUsageMeter = new Meter(new TickerClock(ticker));
    this.threadMXBean = threadMXBean;
    this.threadToLastCpuTime = new ConcurrentHashMap<>();
    this.enabled = threadMXBean.isThreadCpuTimeEnabled();
  }

  public ThreadRegister(ThreadMXBean threadMXBean) {
    this(threadMXBean, Ticker.systemTicker());
  }

  /**
   * Returns a thread factory that automatically registers created threads.
   *
   * <p>The returned factory creates new threads that are automatically registered with the
   * ThreadRegister instance. This allows for easy monitoring and management of thread resources.
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

  @VisibleForTesting
  protected Collection<Thread> getRegistered() {
    return ImmutableSet.copyOf(this.threads);
  }

  @Override
  public void tick() {
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

  @Override
  public double getUsage() {
    if (!enabled) {
      return Double.NaN;
    }

    return cpuUsageMeter.getOneMinuteRate() / NANOS_PER_SECOND;
  }

  private static class TickerClock extends Clock {
    private final Ticker ticker;

    TickerClock(Ticker ticker) {
      this.ticker = ticker;
    }

    @Override
    public long getTick() {
      return ticker.read();
    }
  }
}
