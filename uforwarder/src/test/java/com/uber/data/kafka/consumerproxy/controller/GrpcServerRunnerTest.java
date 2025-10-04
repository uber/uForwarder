package com.uber.data.kafka.consumerproxy.controller;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.ReadStore;
import com.uber.data.kafka.datatransfer.controller.autoscalar.AutoScalarConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rpc.ControllerAdminService;
import com.uber.data.kafka.datatransfer.controller.rpc.ControllerWorkerService;
import com.uber.data.kafka.datatransfer.controller.rpc.JobWorkloadSink;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GrpcServerRunnerTest {

  @Test
  public void testGrpcServerRunner() {
    GrpcServerRunner grpcServerRunner =
        new GrpcServerRunner(
            0,
            new ControllerAdminService(
                Mockito.mock(CoreInfra.class),
                Mockito.mock(Store.class),
                Mockito.mock(Store.class),
                Mockito.mock(Store.class),
                new AutoScalarConfiguration(),
                Mockito.mock(LeaderSelector.class)),
            new ControllerWorkerService(
                Mockito.mock(CoreInfra.class),
                Mockito.mock(Node.class),
                Mockito.mock(Store.class),
                Mockito.mock(ReadStore.class),
                Mockito.mock(Store.class),
                Mockito.mock(LeaderSelector.class),
                Mockito.mock(JobWorkloadSink.class)));
    Assertions.assertFalse(grpcServerRunner.isRunning());
    grpcServerRunner.start();

    Assertions.assertTrue(grpcServerRunner.isRunning());
    Assertions.assertTrue(grpcServerRunner.getPort() > 0);
    grpcServerRunner.stop();
    Assertions.assertFalse(grpcServerRunner.isRunning());
    Assertions.assertEquals(-1, grpcServerRunner.getPort());
  }
}
