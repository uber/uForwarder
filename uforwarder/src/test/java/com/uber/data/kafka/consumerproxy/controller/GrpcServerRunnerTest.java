package com.uber.data.kafka.consumerproxy.controller;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.ReadStore;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rpc.ControllerAdminService;
import com.uber.data.kafka.datatransfer.controller.rpc.ControllerWorkerService;
import com.uber.data.kafka.datatransfer.controller.rpc.JobThroughputSink;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class GrpcServerRunnerTest extends FievelTestBase {

  @Test
  public void testGrpcServerRunner() {
    GrpcServerRunner grpcServerRunner =
        new GrpcServerRunner(
            8088,
            new ControllerAdminService(
                Mockito.mock(CoreInfra.class),
                Mockito.mock(Store.class),
                Mockito.mock(Store.class),
                Mockito.mock(LeaderSelector.class)),
            new ControllerWorkerService(
                Mockito.mock(CoreInfra.class),
                Mockito.mock(Node.class),
                Mockito.mock(Store.class),
                Mockito.mock(ReadStore.class),
                Mockito.mock(Store.class),
                Mockito.mock(LeaderSelector.class),
                Mockito.mock(JobThroughputSink.class)));
    Assert.assertFalse(grpcServerRunner.isRunning());
    grpcServerRunner.start();

    Assert.assertTrue(grpcServerRunner.isRunning());
    grpcServerRunner.stop();
    Assert.assertFalse(grpcServerRunner.isRunning());
  }
}
