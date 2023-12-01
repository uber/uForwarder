package com.uber.data.kafka.datatransfer.common;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

/**
 * ManagedChannelFactory is a helper class to create managedChannel with {@link NettyChannelBuilder}
 */
public interface ManagedChannelFactory {
  ManagedChannelFactory DEFAULT_INSTANCE =
      (host, port) -> NettyChannelBuilder.forAddress(host, port).usePlaintext().build();

  /**
   * Creates managed channel.
   *
   * @param host the host
   * @param port the port
   * @return the managed channel
   */
  ManagedChannel newManagedChannel(String host, int port);
}
