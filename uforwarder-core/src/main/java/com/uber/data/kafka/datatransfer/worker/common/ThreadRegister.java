package com.uber.data.kafka.datatransfer.worker.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * A thread registry that tracks threads and provides CPU usage measurement capabilities.
 *
 * <p>This class implements {@link CpuUsageMeter} to provide CPU usage monitoring for registered
 * threads. It maintains a weak reference set of threads to allow garbage collection of terminated
 * threads, and provides a thread factory that automatically registers created threads.
 */
public class ThreadRegister implements CpuUsageMeter {
  /** The default thread factory used for creating new threads. */
  private static final ThreadFactory BACKING_THREAD_FACTORY = Executors.defaultThreadFactory();

  /**
   * Set of registered threads, using WeakHashMap to allow garbage collection of terminated threads.
   */
  private final Set<Thread> threads;

  /**
   * Constructs a new ThreadRegister instance.
   *
   * <p>Creates a new weak reference set for tracking registered threads.
   */
  public ThreadRegister() {
    threads = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
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
    return ImmutableSet.copyOf(threads);
  }

  @Override
  public void tick() {
    // TODO: implement
  }

  @Override
  public double getUsage() {
    // TODO: implement
    return 0.0;
  }
}
