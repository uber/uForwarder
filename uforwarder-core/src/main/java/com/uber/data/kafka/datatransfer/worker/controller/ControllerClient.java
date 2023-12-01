package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.api.core.InternalApi;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.uber.data.kafka.datatransfer.MasterWorkerServiceGrpc;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.HostResolver;
import com.uber.data.kafka.datatransfer.common.NodeUtils;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import io.grpc.ManagedChannel;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MasterClient is a GRPC client to the master.
 *
 * @implNote we use BlockingStub because is is easier to reason about and we expect low throughput
 *     (~1 request per heartbeat interval) outbound traffic.
 */
@InternalApi
public final class ControllerClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(ControllerClient.class);
  // TODO(haitao.zhang): use 1s for now, figure out a proper value after running it for some time
  private static final long TERMINATION_WAITING_TIME_MS = 1000;
  private final Node node;
  private final ManagedChannel channel;
  private final MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub stub;
  private final CoreInfra infra;

  @VisibleForTesting
  ControllerClient(
      Node node,
      ManagedChannel channel,
      MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub stub,
      CoreInfra infra) {
    this.node = node;
    this.channel = channel;
    this.stub = stub;
    this.infra = infra;
  }

  /**
   * getNode returns the node information for the master.
   *
   * @return Node for the master.
   */
  Node getNode() {
    return node;
  }

  /** @return the gRPC channel that this master client is using to connect to the master. */
  public ManagedChannel getChannel() {
    return channel;
  }

  /**
   * getStub returns the GRPC BlockingStub that can be used to connect to the master.
   *
   * @return MasterWorkerServiceBlockingStub for sending requests to the master.
   */
  MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub getStub() {
    return stub;
  }

  /** Close this master client and gRPC channel. */
  @Override
  public void close() throws IOException {
    if (channel.isTerminated()) {
      return;
    }
    channel.shutdownNow();
    try {
      // wait until the change is terminated to fix the error:
      // *~*~*~ Channel ManagedChannelImpl{logId=XXX, target=<host>:port} was not shutdown
      // properly!!! ~*~*~* Make sure to call shutdown()/shutdownNow() and wait until
      // awaitTermination() returns true.
      long startNs = System.nanoTime();
      if (channel.awaitTermination(TERMINATION_WAITING_TIME_MS, TimeUnit.MILLISECONDS)) {
        infra
            .scope()
            .timer(MetricsNames.GRPC_CHANNEL_TERMINATE_LATENCY)
            .record(com.uber.m3.util.Duration.between(startNs, System.nanoTime()));
        infra.scope().counter(MetricsNames.CLOSE_SUCCESS).inc(1);
        logger.debug(
            "successfully closed master client channel",
            StructuredLogging.masterHostPort(NodeUtils.getHostAndPortString(node)));
      } else {
        infra.scope().counter(MetricsNames.CLOSE_FAILURE).inc(1);
        logger.error(
            "failed to close master client channel",
            StructuredLogging.masterHostPort(NodeUtils.getHostAndPortString(node)),
            StructuredLogging.reason("grpc channel shutdown timeout"));
        throw new IOException("grpc channel shutdown timeout");
      }
    } catch (InterruptedException e) {
      infra.scope().counter(MetricsNames.CLOSE_FAILURE).inc(1);
      logger.error(
          "failed to close master client channel",
          StructuredLogging.masterHostPort(NodeUtils.getHostAndPortString(node)),
          e);
      throw new IOException(e);
    }
  }

  /** Factory uses a provided Resolver to create MasterClient. */
  static final class Factory {
    private final HostResolver masterResolver;
    private final BiFunction<String, Integer, ManagedChannel> managedChannelProvider;
    private final CoreInfra infra;

    Factory(
        HostResolver masterResolver,
        BiFunction<String, Integer, ManagedChannel> managedChannelProvider,
        CoreInfra infra) {
      this.masterResolver = masterResolver;
      this.managedChannelProvider = managedChannelProvider;
      this.infra = infra;
    }

    /**
     * Connect to a new MasterClient using the provided factory resolver.
     *
     * @return MasterClient that can be used to send requests to the master.
     * @throws Exception if MasterClient could not be created.
     * @implNote this function wraps node lookup via masterResolver.
     */
    ControllerClient connect() throws Exception {
      return connect(masterResolver.getHostPort());
    }

    /** Connect to a new master client connected to the provided hostPort. */
    ControllerClient connect(HostAndPort hostPort) {
      return connect(hostPort.getHost(), hostPort.getPort());
    }

    /** Connect to a new master client connected to the provided hostPort. */
    ControllerClient connect(String host, int port) {
      ManagedChannel channel = managedChannelProvider.apply(host, port);
      MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub stub =
          MasterWorkerServiceGrpc.newBlockingStub(channel);
      return new ControllerClient(
          Node.newBuilder().setHost(host).setPort(port).build(), channel, stub, infra);
    }
    /**
     * Reconnects to the provided node if there the node is different from the one the provided
     * client is connected to.
     *
     * @param node that we want to connect to.
     * @param client that is currently connected to the master.
     * @return MasterClient connected to the node.
     * @throws IOException
     */
    ControllerClient reconnectOnChange(ControllerClient client, Node node) throws IOException {
      if (node.equals(client.getNode())) {
        // if client and new nodes are the same, reuse the client.
        return client;
      }

      // Else, master has changed so close client client and create new master client.
      client.close();
      return connect(node.getHost(), node.getPort());
    }

    /**
     * Reconnect to the master.
     *
     * @param client that is currently connected to the master.
     * @return MasterClient connected to the master
     * @throws Exception
     */
    ControllerClient reconnect(ControllerClient client) throws Exception {
      client.close();
      return connect();
    }

    /**
     * Connects to a new MasterClient with default fallback using the provided factory resolver.
     *
     * @return MasterClient that can be used to send requests to the master.
     * @throws Exception if MasterClient could not be created.
     * @implNote this function wraps node lookup via masterResolver.
     */
    ControllerClient connectOrDefault(ControllerClient defaultClient) {
      try {
        return connect();
      } catch (Exception e) {
        return defaultClient;
      }
    }
  }

  private static class MetricsNames {
    static final String CLOSE_SUCCESS = "master-client.close.success";
    static final String CLOSE_FAILURE = "master-client.close.failure";
    static final String GRPC_CHANNEL_TERMINATE_LATENCY =
        "master-client.grpc.channel.terminate.latency";
  }
}
