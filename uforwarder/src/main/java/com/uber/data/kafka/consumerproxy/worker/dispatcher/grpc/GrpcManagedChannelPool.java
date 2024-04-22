package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * GrpcManagedChannelPool implements the gRPC ManagedChannel interface but is backed by multiple
 * actual gRPC ManagedChannels.
 *
 * <p>This is useful b/c muttley imposes a 400 concurrent connection per channel limitation but some
 * use cases with higher latency wants to achieve higher throughput. In general, we should verify
 * concurrency limits with Muttley before imposing them, but this implementation gives us the
 * flexibility to modify the value on our side during emergencies without going through Muttley
 * deployment process which may be slow.
 *
 * <p>Per https://github.com/grpc/grpc-java/issues/2370, gRPC channels connections are lazily
 * maintained so if we can pre-created extra channels without worrying about extra connections. IDLE
 * connections will be automatically closed.
 *
 * <p>TODO: register a metric emitter to the gRPC state change listener.
 */
@ThreadSafe
public class GrpcManagedChannelPool extends ManagedChannel {
  private static final double CRITICAL_USAGE = 0.9;
  private ImmutableChannelPool channelPool;
  private final AtomicInteger concurrentCalls;
  private final Metrics metrics;
  // The value comes from server configuration MAX_CONCURRENT_STREAMS
  private final int maxConcurrentStreams;
  private final Supplier<ManagedChannel> channelProvider;
  private final AtomicBoolean isUpdating;
  private final AtomicBoolean shutdown;
  /**
   * Creates a new GrpcManagedChannelPool for this callee, caller pair.
   *
   * @param channelProvider is the constructor for ManagedChannels in this pool.
   * @param poolSize is the size of the GrpcManagedChannelPool.
   * @param maxConcurrentStreams is the max concurrent streams of HTTP/2
   * @return GrpcManagedChannelPool.
   */
  public GrpcManagedChannelPool(
      Supplier<ManagedChannel> channelProvider, int poolSize, int maxConcurrentStreams) {
    Preconditions.checkArgument(poolSize > 0, "gRPC Channel Pool Size must be > 0");
    this.channelProvider = channelProvider;
    this.channelPool = createChannelPool(channelProvider, poolSize);
    this.concurrentCalls = new AtomicInteger(0);
    this.maxConcurrentStreams = maxConcurrentStreams;
    this.metrics = new MetricsImpl();
    this.isUpdating = new AtomicBoolean(false);
    this.shutdown = new AtomicBoolean(false);
  }

  /**
   * Invokes newCall() on one of the ManagedChannels in the pool. The managed channel is selected
   * via round robin.
   */
  @Override
  public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
    // check and scale channel pool if needed
    maybeScalePool();

    ManagedChannel channelToCall = channelPool.next();
    return new GrpcManagedChannelPoolClientCall(
        channelToCall.newCall(methodDescriptor, callOptions));
  }

  /** Returns the authority of the default channel. */
  @Override
  public String authority() {
    // channel pool must have at least entry.
    return channelPool.iterator().next().authority();
  }

  /** Shutdown all of the channels in the pool. */
  @Override
  public synchronized ManagedChannel shutdown() {
    if (shutdown.compareAndSet(false, true)) {
      Iterator<ManagedChannel> iterator = channelPool.iterator();
      while (iterator.hasNext()) {
        iterator.next().shutdown();
      }
    }

    return this;
  }

  /** Returns true if all of the channels in the pool are shutdown. False otherwise. */
  @Override
  public synchronized boolean isShutdown() {
    return shutdown.get();
  }

  /** Returns true if all of the channels in the pool are terminated. False otherwise. */
  @Override
  public synchronized boolean isTerminated() {
    boolean terminated = true;
    Iterator<ManagedChannel> iterator = channelPool.iterator();
    while (iterator.hasNext()) {
      terminated &= iterator.next().isTerminated();
    }
    return terminated;
  }

  /** Shutdown now all of the channels in the pool. */
  @Override
  public synchronized ManagedChannel shutdownNow() {
    if (shutdown.compareAndSet(false, true)) {
      Iterator<ManagedChannel> iterator = channelPool.iterator();
      while (iterator.hasNext()) {
        iterator.next().shutdownNow();
      }
    }
    return this;
  }

  /** Await termination of all channels in the pool. */
  @Override
  public synchronized boolean awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException {
    long remainingAwaitNs = unit.toNanos(timeout);

    boolean terminated = true;
    long startNs = System.nanoTime();
    Iterator<ManagedChannel> iterator = channelPool.iterator();
    while (iterator.hasNext()) {
      terminated &= iterator.next().awaitTermination(remainingAwaitNs, TimeUnit.NANOSECONDS);
      remainingAwaitNs -= System.nanoTime() - startNs;
      startNs = System.nanoTime();
    }

    return terminated;
  }

  /**
   * Gets metrics.
   *
   * @return the metrics
   */
  public Metrics getMetrics() {
    return metrics;
  }

  /** Check usage of the pool, scale out the pool if usage above threshold */
  private void maybeScalePool() {
    if (!isShutdown() && metrics.usage() > CRITICAL_USAGE) {
      if (isUpdating.compareAndSet(false, true)) {
        try {
          synchronized (this) {
            if (!isShutdown()) {
              channelPool = channelPool.resize(channelPool.size() + 1);
            }
          }
        } finally {
          isUpdating.set(false);
        }
      }
    }
  }

  private ImmutableChannelPool createChannelPool(
      Supplier<ManagedChannel> channelProvider, int poolSize) {
    ImmutableList.Builder<ManagedChannel> poolBuilder = ImmutableList.builder();
    for (int i = 0; i < poolSize; i++) {
      poolBuilder.add(channelProvider.get());
    }
    return new ImmutableChannelPool(poolBuilder.build());
  }

  /** Metrics of {@link GrpcManagedChannelPool} */
  public interface Metrics {

    /**
     * Gets channel pool usage. channel pool usage is ratio between active calls and total active
     * calls channel pool can support see ref, https://grpc.io/docs/guides/performance/
     *
     * @return the double indicates ratio of active calls in max active calls the pool can support
     */
    double usage();

    /**
     * Gets inflight request count
     *
     * @return the int indicates inflight request count
     */
    int inflight();

    /**
     * Gets channel pool size
     *
     * @return the channel pool size
     */
    int size();
  }

  /** Threadsafe channel pool */
  private class ImmutableChannelPool {
    private final ImmutableList<ManagedChannel> channels;
    private final int size;
    private final AtomicInteger index;

    ImmutableChannelPool(ImmutableList<ManagedChannel> channels) {
      this.channels = channels;
      this.size = channels.size();
      this.index = new AtomicInteger(0);
    }

    private ManagedChannel next() {
      // channelPoolSize is an int so it is safe to cast the output of mod to int.
      return channels.get(Math.abs(index.getAndIncrement()) % size);
    }

    private UnmodifiableIterator<ManagedChannel> iterator() {
      return channels.iterator();
    }

    private int size() {
      return size;
    }

    private ImmutableChannelPool resize(int newSize) {
      newSize = Math.max(newSize, 1);

      if (newSize == size) {
        return this;
      }

      ImmutableList.Builder builder = new ImmutableList.Builder();

      if (newSize > size) {
        // up scale
        builder.addAll(channels);
        for (int i = 0; i < newSize - size; ++i) {
          builder.add(channelProvider.get());
        }
      }
      // TODO: implement down scale

      return new ImmutableChannelPool(builder.build());
    }
  }

  private class MetricsImpl implements Metrics {

    @Override
    public double usage() {
      return Math.min(
          1.0, (double) concurrentCalls.get() / (channelPool.size() * maxConcurrentStreams));
    }

    @Override
    public int inflight() {
      return concurrentCalls.get();
    }

    @Override
    public int size() {
      return channelPool.size();
    }
  }

  private class GrpcManagedChannelPoolClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
    private ClientCall<ReqT, RespT> delegator;

    GrpcManagedChannelPoolClientCall(ClientCall<ReqT, RespT> delegator) {
      this.delegator = delegator;
    }

    @Override
    public void start(Listener<RespT> listener, Metadata metadata) {
      ListenerAdapter poolListener = new ListenerAdapter(listener);
      try {
        delegator.start(poolListener, metadata);
      } catch (Throwable t) {
        // start client call could fail with exception when call already canceled
        // need to close listener to avoid inflight leaking
        poolListener.close();
        throw t;
      }
    }

    @Override
    public void request(int i) {
      delegator.request(i);
    }

    @Override
    public void cancel(@Nullable String s, @Nullable Throwable throwable) {
      delegator.cancel(s, throwable);
    }

    @Override
    public void halfClose() {
      delegator.halfClose();
    }

    @Override
    public void sendMessage(ReqT reqT) {
      delegator.sendMessage(reqT);
    }
  }

  private class ListenerAdapter<T> extends ClientCall.Listener<T> {
    private final ClientCall.Listener<T> delegator;
    private final AtomicBoolean closed;

    ListenerAdapter(ClientCall.Listener<T> delegator) {
      super();
      this.delegator = delegator;
      this.closed = new AtomicBoolean(false);
      concurrentCalls.incrementAndGet();
    }

    public void onHeaders(Metadata headers) {
      delegator.onHeaders(headers);
    }

    public void onMessage(T message) {
      delegator.onMessage(message);
    }

    public void onClose(Status status, Metadata trailers) {
      close();
      delegator.onClose(status, trailers);
    }

    public void onReady() {
      delegator.onReady();
    }

    private boolean close() {
      if (closed.compareAndSet(false, true)) {
        concurrentCalls.decrementAndGet();
        return true;
      }
      return false;
    }
  }
}
