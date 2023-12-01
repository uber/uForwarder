package com.uber.data.kafka.consumerproxy.testutils;

import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.model.ContainerNetwork;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/** Utilities for network related stuffs during unit tests */
public class NetworkUtils {

  /**
   * Validates the port is in use
   *
   * @param port the port
   */
  public static void assertPortInUse(int port) {
    try (ServerSocket socket = new ServerSocket(port)) {
      Assert.fail(String.format("Fail to start up service on port %d endpoint", port));
    } catch (IOException ioe) {
    }
  }

  public static int getRandomAvailablePort() {
    try (ServerSocket socket = new ServerSocket(0); ) {
      return socket.getLocalPort();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Validates the port is in use
   *
   * @param maxAwaitTimeInSec the max time to wait for the port to be use
   * @param port the port
   */
  public static void assertPortInUseWithTimeout(int port, int maxAwaitTimeInSec) {
    await()
        .atMost(maxAwaitTimeInSec, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertPortInUse(port);
            });
  }

  /**
   * Validates the port is free
   *
   * @param port the port
   */
  public static void assertPortIsFree(int port) {
    try (ServerSocket socket = new ServerSocket(port)) {
    } catch (IOException ioe) {
      Assert.fail(ioe.getMessage());
    }
  }

  /**
   * Gets IP address of given broker and network
   *
   * @param container
   * @param network
   * @return
   */
  public static String getIpAddress(GenericContainer container, Network network) {
    Optional<ContainerNetwork> matchingNetwork =
        container
            .getContainerInfo()
            .getNetworkSettings()
            .getNetworks()
            .values()
            .stream()
            .filter(n -> n.getNetworkID().equals(network.getId()))
            .findAny();

    return matchingNetwork.map(ContainerNetwork::getIpAddress).get();
  }
}
