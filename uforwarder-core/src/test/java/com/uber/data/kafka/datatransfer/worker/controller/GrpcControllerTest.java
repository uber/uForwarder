package com.uber.data.kafka.datatransfer.worker.controller;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.HostResolver;
import com.uber.data.kafka.datatransfer.common.ManagedChannelFactory;
import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GrpcControllerTest {
  private GrpcControllerConfiguration config;
  private CoreInfra infra;
  private Node worker;
  private HostResolver resolver;
  private Controllable controllable;
  private GrpcController grpcController;
  private ManagedChannelFactory managedChannelFactory;

  @BeforeEach
  public void setup() {
    config = new GrpcControllerConfiguration();
    infra = CoreInfra.NOOP;
    worker = Node.newBuilder().setId(1).setHost("localhost").setPort(1000).build();
    resolver = Mockito.mock(HostResolver.class);
    controllable = Mockito.mock(Controllable.class);
    managedChannelFactory = ManagedChannelFactory.DEFAULT_INSTANCE;
    grpcController =
        new GrpcController(config, infra, worker, resolver, controllable, managedChannelFactory);
  }

  @Test
  public void lifecycle() {
    grpcController.start();
    Assertions.assertTrue(grpcController.isRunning());
    grpcController.stop();
  }

  @Test
  public void run() {
    Assertions.assertTrue(grpcController.getState() instanceof StateConnecting);
    grpcController.run();
    Assertions.assertTrue(grpcController.getState() instanceof StateConnecting);
  }
}
