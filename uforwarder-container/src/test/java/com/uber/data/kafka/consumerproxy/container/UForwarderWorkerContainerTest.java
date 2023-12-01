package com.uber.data.kafka.consumerproxy.container;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.utility.DockerImageName;

public class UForwarderWorkerContainerTest extends FievelTestBase {
  private UForwarderWorkerContainer workerContainer;

  @Before
  public void setup() {
    workerContainer = new UForwarderWorkerContainer(DockerImageName.parse("uforwarder"));
    Assert.assertEquals("uforwarder-worker", workerContainer.getEnvMap().get("UFORWARDER_PROFILE"));
  }

  @Test
  public void testSetupControllerConnect() {
    String controllerConnect = "127.0.0.1:1212";
    workerContainer.withController(controllerConnect);
    Assert.assertEquals(
        controllerConnect, workerContainer.getEnvMap().get("UFORWARDER_CONTROLLER_CONNECT"));
  }

  @Test
  public void testSetupMemoryLimit() {
    workerContainer.withMaxMemoryMB(1024);
    Assert.assertEquals("1024", workerContainer.getEnvMap().get("UFORWARDER_MEMORY_LIMIT_MB"));
  }
}
