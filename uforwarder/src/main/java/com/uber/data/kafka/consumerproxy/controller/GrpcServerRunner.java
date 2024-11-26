package com.uber.data.kafka.consumerproxy.controller;

import com.uber.data.kafka.datatransfer.controller.rpc.ControllerAdminService;
import com.uber.data.kafka.datatransfer.controller.rpc.ControllerWorkerService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/** This is a runnable to start GRPC Server */
public class GrpcServerRunner implements SmartLifecycle {

  private static final Logger logger = LoggerFactory.getLogger(GrpcServerRunner.class);
  private static final int NON_SERVING_PORT = -1;

  private final Server server;

  private final AtomicInteger servingPort; // positive port indicates running

  /** Constructor */
  public GrpcServerRunner(
      int port,
      ControllerAdminService controllerAdminService,
      ControllerWorkerService controllerWorkerService) {
    server =
        NettyServerBuilder.forPort(port)
            .addService(controllerAdminService)
            .addService(controllerWorkerService)
            .build();
    this.servingPort = new AtomicInteger(NON_SERVING_PORT);
  }

  @Override
  public void start() {
    try {
      server.start();
    } catch (IOException e) {
      logger.error("Failed to start GrpcServerRunner", e);
      throw new RuntimeException(e);
    }
    servingPort.set(server.getPort());
  }

  @Override
  public void stop() {
    server.shutdown();
    servingPort.set(-1);
  }

  @Override
  public boolean isRunning() {
    return servingPort.get() != NON_SERVING_PORT;
  }

  public int getPort() {
    return servingPort.get();
  }
}
