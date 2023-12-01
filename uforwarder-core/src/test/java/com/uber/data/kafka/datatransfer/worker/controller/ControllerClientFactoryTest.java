package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.common.net.HostAndPort;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.HostResolver;
import com.uber.data.kafka.datatransfer.common.ManagedChannelFactory;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ControllerClientFactoryTest extends FievelTestBase {
  private HostResolver resolver;
  private ControllerClient.Factory factory;

  @Before
  public void setup() throws Exception {
    resolver = Mockito.mock(HostResolver.class);
    factory =
        new ControllerClient.Factory(
            resolver, ManagedChannelFactory.DEFAULT_INSTANCE::newManagedChannel, CoreInfra.NOOP);

    Mockito.when(resolver.getHostPort()).thenReturn(HostAndPort.fromParts("localhost", 1234));
  }

  @Test
  public void testConnect() throws Exception {
    ControllerClient controllerClient = factory.connect();
    Assert.assertEquals("localhost", controllerClient.getNode().getHost());
    Assert.assertEquals(1234, controllerClient.getNode().getPort());
  }

  @Test
  public void testConnectWithOldClient() throws Exception {
    Node oldNode = Node.newBuilder().setId(1).setHost("localhost").setPort(1234).build();
    Node newOld = Node.newBuilder().setId(2).setHost("localhost").setPort(5678).build();
    ControllerClient oldClient = Mockito.mock(ControllerClient.class);
    Mockito.when(oldClient.getNode()).thenReturn(oldNode);

    ControllerClient controllerClient = factory.reconnectOnChange(oldClient, oldNode);
    Assert.assertEquals(oldClient, controllerClient);

    controllerClient = factory.reconnectOnChange(oldClient, newOld);
    Assert.assertNotEquals(oldClient, controllerClient);
  }

  @Test
  public void testConnectWithDefault() throws Exception {
    ControllerClient defaultClient = Mockito.mock(ControllerClient.class);
    ControllerClient newClient = factory.connectOrDefault(defaultClient);
    Assert.assertNotEquals(newClient, defaultClient);

    Mockito.doThrow(new RuntimeException()).when(resolver).getHostPort();
    newClient = factory.connectOrDefault(defaultClient);
    Assert.assertEquals(newClient, defaultClient);
  }

  @Test
  public void testReconnect() throws Exception {
    ControllerClient oldClient = Mockito.mock(ControllerClient.class);
    ControllerClient newClient = factory.reconnect(oldClient);
    Assert.assertEquals("localhost", newClient.getNode().getHost());
    Assert.assertEquals(1234, newClient.getNode().getPort());
  }
}
