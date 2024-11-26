package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.google.common.annotations.VisibleForTesting;
import java.util.AbstractQueue;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Supports acquire permit asynchronously use nested {@link InflightLimiter} as delegator Note,
 * adapter needs to be closed independently from delegator
 */
public class AsyncInflightLimiterAdapter implements AutoCloseable {
  private final Queue<PermitCompletableFuture> futurePermits = new PendingMessageQueue();
  private final AtomicInteger pendingPermitCount = new AtomicInteger();
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private final InflightLimiter delegator;
  private final Metrics metrics;

  private AsyncInflightLimiterAdapter(InflightLimiter delegator) {
    this.delegator = delegator;
    this.metrics = new Metrics();
  }

  /**
   * Acquires a permit asynchronously. if there is permit, return a complete future. otherwise
   * future enters a priority queue, when dequeue, it should round-robin travers each partition.
   * with-in each partition, message order by offset
   *
   * @param partition the partition
   * @param offset the offset
   * @return completable future
   */
  public CompletableFuture<InflightLimiter.Permit> acquireAsync(
      int partition, long offset, boolean dryRun) {
    if (futurePermits.isEmpty()) {
      // if there is no queue, return a completed future
      Optional<InflightLimiter.Permit> permit = delegator.tryAcquire(dryRun);
      if (permit.isPresent()) {
        return CompletableFuture.completedFuture(new AsyncPermit(permit.get()));
      }
    }
    // add future into queue and try to complete one
    PermitCompletableFuture result = new PermitCompletableFuture(partition, offset);
    synchronized (this) {
      futurePermits.add(result);
    }
    tryCompleteFuture();
    return result;
  }

  @Override
  public void close() {
    if (isClosed.compareAndSet(false, true)) {
      /** complete all future permits */
      synchronized (this) {
        PermitCompletableFuture future;
        while ((future = futurePermits.poll()) != null) {
          future.complete(InflightLimiter.NoopPermit.INSTANCE);
        }
      }
    }
  }

  /**
   * Gets metrics the adapter
   *
   * @return
   */
  public Metrics getMetrics() {
    return metrics;
  }

  /**
   * Creates new instance of adapter
   *
   * @param delegator
   * @return
   */
  public static AsyncInflightLimiterAdapter of(InflightLimiter delegator) {
    return new AsyncInflightLimiterAdapter(delegator);
  }

  private void tryCompleteFuture() {
    // try to complete any future permit if there is

    if (futurePermits.isEmpty()) {
      return;
    }

    Runnable runnable = null;
    synchronized (AsyncInflightLimiterAdapter.this) {
      if (futurePermits.isEmpty()) {
        return;
      }
      Optional<InflightLimiter.Permit> permit = delegator.tryAcquire();
      if (permit.isPresent()) {
        CompletableFuture<InflightLimiter.Permit> future;
        // purge completed futures and find one pending future on the head of queue
        while ((future = futurePermits.poll()) != null) {
          if (!future.isDone()) {
            final CompletableFuture<InflightLimiter.Permit> finalFuture = future;
            // reduce scope of critical zone by delay completion
            runnable = () -> finalFuture.complete(new AsyncPermit(permit.get()));
            break;
          }
        }
        if (runnable == null) {
          // complete the permit to avoid leaking
          runnable = () -> permit.get().complete();
        }
      }
    }
    if (runnable != null) {
      runnable.run();
    }
  }

  private class AsyncPermit implements InflightLimiter.Permit {
    private final InflightLimiter.Permit nestedPermit;

    AsyncPermit(InflightLimiter.Permit permit) {
      nestedPermit = permit;
    }

    @Override
    public boolean complete(InflightLimiter.Result result) {
      boolean succeed = nestedPermit.complete(result);
      if (!succeed) {
        return false;
      }
      tryCompleteFuture();
      return true;
    }
  }

  class PermitCompletableFuture extends CompletableFuture<InflightLimiter.Permit>
      implements Comparable<PermitCompletableFuture> {
    private final int partition;
    private final long offset;

    private PermitCompletableFuture(int partition, long offset) {
      this.partition = partition;
      this.offset = offset;
      pendingPermitCount.incrementAndGet();
    }

    @Override
    public boolean complete(InflightLimiter.Permit permit) {
      boolean completed = super.complete(permit);
      if (completed) {
        pendingPermitCount.decrementAndGet();
      } else {
        // avoid leaking of permit
        permit.complete();
      }
      return completed;
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
      boolean completed = super.completeExceptionally(ex);
      if (completed) {
        pendingPermitCount.decrementAndGet();
      }
      return completed;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      // complete with noop permit
      return complete(InflightLimiter.NoopPermit.INSTANCE);
    }

    @Override
    public int compareTo(PermitCompletableFuture o) {
      return Long.compare(offset, o.offset);
    }
  }

  /**
   * Queue for pending message when dequeue, partitions are selected in round-robin way. messages
   * are sorted by offset per partition
   */
  @VisibleForTesting
  class PendingMessageQueue extends AbstractQueue<PermitCompletableFuture> {
    private final Map<Integer, PriorityQueue<PermitCompletableFuture>> queues;
    private Iterator<Map.Entry<Integer, PriorityQueue<PermitCompletableFuture>>> queueIterator;

    private AtomicInteger size = new AtomicInteger(0);

    PendingMessageQueue() {
      queues = new HashMap<>();
      queueIterator = newIterator();
    }

    @Override
    public Iterator<PermitCompletableFuture> iterator() {
      throw new UnsupportedOperationException();
    }

    /** Gets size of the queue */
    @Override
    public int size() {
      return size.get();
    }

    @Override
    public boolean offer(PermitCompletableFuture future) {
      PriorityQueue<PermitCompletableFuture> queue;
      if (queues.containsKey(future.partition)) {
        queue = queues.get(future.partition);
      } else {
        queue = new PriorityQueue<>();
        queues.put(future.partition, queue);
        queueIterator = newIterator();
      }
      boolean result = queue.offer(future);
      if (result) {
        size.incrementAndGet();
      }
      return result;
    }

    @Override
    public @Nullable PermitCompletableFuture poll() {
      Optional<Map.Entry<Integer, PriorityQueue<PermitCompletableFuture>>> entry = nextEntry();
      if (entry.isPresent()) {
        PriorityQueue<PermitCompletableFuture> queue = entry.get().getValue();
        PermitCompletableFuture result = queue.poll();
        if (result != null) {
          size.decrementAndGet();
        }
        if (queue.isEmpty()) {
          queues.remove(entry.get().getKey());
          queueIterator = newIterator();
        }
        return result;
      }
      return null;
    }

    @Override
    public PermitCompletableFuture peek() {
      throw new UnsupportedOperationException();
    }

    private Iterator<Map.Entry<Integer, PriorityQueue<PermitCompletableFuture>>> newIterator() {
      return queues.entrySet().iterator();
    }

    private Optional<Map.Entry<Integer, PriorityQueue<PermitCompletableFuture>>> nextEntry() {
      Optional<Map.Entry<Integer, PriorityQueue<PermitCompletableFuture>>> result;
      if (queueIterator.hasNext()) {
        result = Optional.of(queueIterator.next());
        if (!queueIterator.hasNext()) {
          // circulate entries
          queueIterator = newIterator();
        }
        return result;
      }
      return Optional.empty();
    }
  }

  /** Adapter metrics */
  public class Metrics {

    /**
     * Gets number of requests to be completed
     *
     * @return
     */
    public long getAsyncQueueSize() {
      return pendingPermitCount.get();
    }

    /**
     * Gets number of requests in the pending queue Some requests may have already been completed
     *
     * @return
     */
    public int getPendingQueueSize() {
      return futurePermits.size();
    }
  }
}
