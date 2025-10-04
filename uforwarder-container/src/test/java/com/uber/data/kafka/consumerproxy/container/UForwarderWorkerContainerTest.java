package com.uber.data.kafka.consumerproxy.container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

public class UForwarderWorkerContainerTest {
  private UForwarderWorkerContainer workerContainer;

  @BeforeEach
  public void setup() {
    workerContainer = new UForwarderWorkerContainer(DockerImageName.parse("uforwarder"));
    Assertions.assertEquals(
        "uforwarder-worker", workerContainer.getEnvMap().get("UFORWARDER_PROFILE"));
  }

  @Test
  public void testSetupControllerConnect() {
    String controllerConnect = "127.0.0.1:1212";
    workerContainer.withController(controllerConnect);
    Assertions.assertEquals(
        controllerConnect, workerContainer.getEnvMap().get("UFORWARDER_CONTROLLER_CONNECT"));
  }

  @Test
  public void testSetupMemoryLimit() {
    workerContainer.withMaxMemoryMB(1024);
    Assertions.assertEquals("1024", workerContainer.getEnvMap().get("UFORWARDER_MEMORY_LIMIT_MB"));
  }
}
