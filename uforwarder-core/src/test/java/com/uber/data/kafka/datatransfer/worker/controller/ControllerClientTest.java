package com.uber.data.kafka.datatransfer.worker.controller;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.data.kafka.datatransfer.MasterWorkerServiceGrpc;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import io.grpc.ManagedChannel;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class ControllerClientTest {
  private Node node;
  private ManagedChannel managedChannel;
  private MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub stub;
  private ControllerClient client;

  @BeforeEach
  public void setup() {
    node = Node.newBuilder().setId(1).setHost("foo").setPort(1).build();
    managedChannel = Mockito.mock(ManagedChannel.class);
    stub = Mockito.mock(MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub.class);
    client = new ControllerClient(node, managedChannel, stub, CoreInfra.NOOP);
  }

  @Test
  public void testGetNode() {
    Assertions.assertEquals(node, client.getNode());
  }

  @Test
  public void testGetChannel() {
    Assertions.assertEquals(managedChannel, client.getChannel());
  }

  @Test
  public void testGetStub() {
    Assertions.assertEquals(stub, client.getStub());
  }

  @Test
  public void testClose() throws IOException, InterruptedException {
    Mockito.when(
            managedChannel.awaitTermination(ArgumentMatchers.anyLong(), ArgumentMatchers.any()))
        .thenReturn(true);
    client.close();
  }

  @Test
  public void testCloseWithTimeout() throws Exception {
    assertThrows(IOException.class, () -> client.close());
  }

  @Test
  public void testCloseWithException() throws InterruptedException {
    assertThrows(
        IOException.class,
        () -> {
          Mockito.when(
                  managedChannel.awaitTermination(
                      ArgumentMatchers.anyLong(), ArgumentMatchers.any()))
              .thenThrow(new InterruptedException());
          client.close();
        });
  }

  @Test
  public void testCloseWithGrpcChannelTerminated() throws IOException {
    Mockito.when(managedChannel.isTerminated()).thenReturn(true);
    client.close();
  }
}
