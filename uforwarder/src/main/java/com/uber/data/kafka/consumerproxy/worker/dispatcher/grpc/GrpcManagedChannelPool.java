package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
public final class GrpcManagedChannelPool extends ManagedChannel {
  private final List<ManagedChannel> channelPool;
  private final AtomicLong index;

  @VisibleForTesting
  GrpcManagedChannelPool(List<ManagedChannel> channelPool, AtomicLong index) {
    Preconditions.checkArgument(channelPool.size() > 0, "gRPC Channel Pool Size must be > 0");
    this.channelPool = channelPool;
    this.index = index;
  }

  /**
   * Creates a new GrpcManagedChannelPool for this callee, caller pair.
   *
   * @param channelProvider is the constructor for ManagedChannels in this pool.
   * @param poolSize is the size of the GrpcManagedChannelPool.
   * @return GrpcManagedChannelPool.
   */
  public static GrpcManagedChannelPool of(
      Function<GrpcChannelBuilderOptions, ManagedChannel> channelProvider, int poolSize) {
    ImmutableList.Builder<ManagedChannel> poolBuilder = ImmutableList.builder();
    for (int i = 0; i < poolSize; i++) {
      poolBuilder.add(channelProvider.apply(GrpcChannelBuilderOptions.DEFAULT));
    }
    return new GrpcManagedChannelPool(poolBuilder.build(), new AtomicLong(0));
  }

  /**
   * Invokes newCall() on one of the ManagedChannels in the pool. The managed channel is selected
   * via round robin.
   */
  @Override
  public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
      MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
    long i = index.getAndIncrement();

    // channelPoolSize is an int so it is safe to cast the output of mod to int.
    ManagedChannel channelToCall = channelPool.get((int) (i % channelPool.size()));
    return channelToCall.newCall(methodDescriptor, callOptions);
  }

  /** Returns the authority of the default channel. */
  @Override
  public String authority() {
    // channel pool must have at least entry.
    return channelPool.get(0).authority();
  }

  /** Shutdown all of the channels in the pool. */
  @Override
  public ManagedChannel shutdown() {
    for (ManagedChannel channel : channelPool) {
      channel.shutdown();
    }
    return this;
  }

  /** Returns true if all of the channels in the pool are shutdown. False otherwise. */
  @Override
  public boolean isShutdown() {
    boolean shutdown = true;
    for (ManagedChannel channel : channelPool) {
      shutdown &= channel.isShutdown();
    }
    return shutdown;
  }

  /** Returns true if all of the channels in the pool are terminated. False otherwise. */
  @Override
  public boolean isTerminated() {
    boolean terminated = true;
    for (ManagedChannel channel : channelPool) {
      terminated &= channel.isTerminated();
    }
    return terminated;
  }

  /** Shutdown now all of the channels in the pool. */
  @Override
  public ManagedChannel shutdownNow() {
    for (ManagedChannel channel : channelPool) {
      channel.shutdownNow();
    }
    return this;
  }

  /** Await termination of all channels in the pool. */
  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long remainingAwaitNs = unit.toNanos(timeout);

    boolean terminated = true;
    long startNs = System.nanoTime();
    for (ManagedChannel channel : channelPool) {
      terminated &= channel.awaitTermination(remainingAwaitNs, TimeUnit.NANOSECONDS);
      remainingAwaitNs -= System.nanoTime() - startNs;
      startNs = System.nanoTime();
    }

    return terminated;
  }
}
