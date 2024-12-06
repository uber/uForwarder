package com.uber.data.kafka.consumerproxy.controller;

import com.uber.data.kafka.datatransfer.controller.rpc.ControllerAdminService;
import com.uber.data.kafka.datatransfer.controller.rpc.ControllerWorkerService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/** This is a runnable to start GRPC Server */
public class GrpcServerRunner implements SmartLifecycle {

  private static final Logger logger = LoggerFactory.getLogger(GrpcServerRunner.class);
  private final AtomicBoolean running;

  private final Server server;

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
    this.running = new AtomicBoolean(false);
  }

  @Override
  public void start() {
    try {
      server.start();
    } catch (IOException e) {
      logger.error("Failed to start GrpcServerRunner", e);
      throw new RuntimeException(e);
    }
    running.set(true);
  }

  @Override
  public void stop() {
    server.shutdown();
    running.set(false);
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }
}
