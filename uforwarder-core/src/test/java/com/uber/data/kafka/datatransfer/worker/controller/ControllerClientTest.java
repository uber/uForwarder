package com.uber.data.kafka.datatransfer.worker.controller;

import com.uber.data.kafka.datatransfer.MasterWorkerServiceGrpc;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.ManagedChannel;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class ControllerClientTest extends FievelTestBase {
  private Node node;
  private ManagedChannel managedChannel;
  private MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub stub;
  private ControllerClient client;

  @Before
  public void setup() {
    node = Node.newBuilder().setId(1).setHost("foo").setPort(1).build();
    managedChannel = Mockito.mock(ManagedChannel.class);
    stub = Mockito.mock(MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub.class);
    client = new ControllerClient(node, managedChannel, stub, CoreInfra.NOOP);
  }

  @Test
  public void testGetNode() {
    Assert.assertEquals(node, client.getNode());
  }

  @Test
  public void testGetChannel() {
    Assert.assertEquals(managedChannel, client.getChannel());
  }

  @Test
  public void testGetStub() {
    Assert.assertEquals(stub, client.getStub());
  }

  @Test
  public void testClose() throws IOException, InterruptedException {
    Mockito.when(
            managedChannel.awaitTermination(ArgumentMatchers.anyLong(), ArgumentMatchers.any()))
        .thenReturn(true);
    client.close();
  }

  @Test(expected = IOException.class)
  public void testCloseWithTimeout() throws Exception {
    client.close();
  }

  @Test(expected = IOException.class)
  public void testCloseWithException() throws InterruptedException, IOException {
    Mockito.when(
            managedChannel.awaitTermination(ArgumentMatchers.anyLong(), ArgumentMatchers.any()))
        .thenThrow(new InterruptedException());
    client.close();
  }

  @Test
  public void testCloseWithGrpcChannelTerminated() throws IOException {
    Mockito.when(managedChannel.isTerminated()).thenReturn(true);
    client.close();
  }
}
