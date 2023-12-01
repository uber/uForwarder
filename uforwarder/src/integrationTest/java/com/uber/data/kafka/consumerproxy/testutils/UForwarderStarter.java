package com.uber.data.kafka.consumerproxy.testutils;

import com.uber.data.kafka.consumerproxy.UForwarder;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for startup in-proc kafka consumer proxy master anc worker */
public class UForwarderStarter {

  private static final Logger logger = LoggerFactory.getLogger(UForwarderStarter.class);

  /**
   * Starts in-proc kafka consumer proxy worker
   *
   * @param bootstrapServers kafka broker bootstrap servers
   * @param httpPort the http port for consumer proxy worker
   * @throws Exception
   */
  public static void startUForwarderWorker(
      String bootstrapServers, int httpPort, String controllerConnectString) throws Exception {
    startUForwarder(
        "uforwarder-worker",
        httpPort,
        new String[] {
          "--worker.dispatcher.kafka.bootstrapServers=" + bootstrapServers,
          "--worker.fetcher.kafka.bootstrapServers=" + bootstrapServers,
          "--server.port=" + httpPort,
          "--system.port=0",
          "--worker.controller.grpc.masterHostPort=" + controllerConnectString
        });
  }

  /**
   * Starts in-proc kafka consumer proxy master
   *
   * @param bootstrapServers kafka broker bootstrap servers
   * @param grpcPort the grpc port for consumer proxy master
   * @throws Exception
   */
  public static void startUForwarderMaster(
      String bootstrapServers, String zkConnectString, int grpcPort) throws Exception {
    UForwarderUtils.prepareZookeeperForController(zkConnectString);
    startUForwarder(
        "uforwarder-controller",
        grpcPort,
        new String[] {
          "--master.zookeeper.zkConnection=" + zkConnectString + Constants.UFORWARDER_ZK_ROOT,
          "--master.kafka.admin.bootstrapServers=" + bootstrapServers,
          "--master.kafka.offsetCommitter.bootstrapServers=" + bootstrapServers,
          "--grpc.port=" + grpcPort,
          "--server.port=0",
        });
  }

  private static void startUForwarder(String profile, int port, String[] parameters)
      throws Exception {
    Thread uForwarderApplication =
        new Thread(
            () -> {
              try {
                UForwarder.main(ArrayUtils.addAll(new String[] {profile}, parameters));
              } catch (Exception e) {
                logger.warn("Fail to start {}", profile);
              }
            });
    uForwarderApplication.start();
    NetworkUtils.assertPortInUseWithTimeout(port, Constants.MAX_AWAIT_TIME_IN_SEC);
    logger.info("{} started on port {}", profile, port);
  }
}
