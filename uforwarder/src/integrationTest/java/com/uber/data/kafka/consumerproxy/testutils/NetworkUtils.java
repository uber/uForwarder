package com.uber.data.kafka.consumerproxy.testutils;

import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.model.ContainerNetwork;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/** Utilities for network related stuffs during unit tests */
public class NetworkUtils {
  private static final Logger logger = LoggerFactory.getLogger(NetworkUtils.class);
  private static final int MAX_RANDOM_PORT_ATTEMPT = 3; // number of attempts to find random port
  private static final int MIN_RANDOM_PORT_OFFSET = 100; // start offset of random port
  private static final int MAX_RANDOM_PORT_OFFSET = 1000; // end offset of randoem port
  private static final int MAX_RANDOM_PORT = 65535; // max port supported
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

  public static synchronized int getRandomAvailablePort() {
    int availablePort;
    try (ServerSocket socket = new ServerSocket(0); ) {
      availablePort = socket.getLocalPort();
    } catch (IOException e) {
      logger.warn("Failed to open socket on port 0", e);
      throw new IllegalStateException(e);
    }

    for (int i = 0; i < MAX_RANDOM_PORT_ATTEMPT; ++i) {
      int port =
          Math.min(
              availablePort + RandomUtils.nextInt(MIN_RANDOM_PORT_OFFSET, MAX_RANDOM_PORT_OFFSET),
              MAX_RANDOM_PORT);
      try (ServerSocket socket = new ServerSocket(port)) {
        int localPort = socket.getLocalPort();
        // Set this system property to ensure static random ports provided by this
        // method is not blocked by the SunNioNetInterceptor which blocks static ports
        // usage at test runtime.
        System.setProperty("com.uber.fievel.testing.available.port." + localPort, "true");
        return localPort;
      } catch (IOException e) {
        logger.warn(String.format("Failed to open socket on port %d, attempt = %d", port, i), e);
        // retry if port is not available
      }
    }

    throw new IllegalStateException("Failed to find available port");
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
      Assert.fail(ioe.getMessage() + " port=" + port);
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
