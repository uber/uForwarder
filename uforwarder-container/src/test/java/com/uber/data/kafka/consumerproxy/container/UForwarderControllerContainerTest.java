package com.uber.data.kafka.consumerproxy.container;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.utility.DockerImageName;

public class UForwarderControllerContainerTest extends FievelTestBase {
  private UForwarderControllerContainer controllerContainer;

  @Before
  public void setup() {
    controllerContainer = new UForwarderControllerContainer(DockerImageName.parse("uforwarder"));
    Assert.assertEquals(
        "uforwarder-controller", controllerContainer.getEnvMap().get("UFORWARDER_PROFILE"));
  }

  @Test
  public void testSetupZkConnect() {
    String zkConnect = "127.0.0.1:2181";
    controllerContainer.withZookeeperConnect(zkConnect);
    Assert.assertEquals(
        zkConnect, controllerContainer.getEnvMap().get("UFORWARDER_ZOOKEEPER_CONNECT"));
  }

  @Test
  public void testSetupMemoryLimit() {
    controllerContainer.withMaxMemoryMB(1024);
    Assert.assertEquals("1024", controllerContainer.getEnvMap().get("UFORWARDER_MEMORY_LIMIT_MB"));
  }

  @Test(expected = IllegalStateException.class)
  public void testGetAddress() {
    controllerContainer.getAddress();
  }
}
