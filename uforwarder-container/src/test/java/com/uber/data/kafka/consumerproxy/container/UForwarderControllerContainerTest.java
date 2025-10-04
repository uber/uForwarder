package com.uber.data.kafka.consumerproxy.container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.utility.DockerImageName;

public class UForwarderControllerContainerTest {
  private UForwarderControllerContainer controllerContainer;

  @BeforeEach
  public void setup() {
    controllerContainer = new UForwarderControllerContainer(DockerImageName.parse("uforwarder"));
    Assertions.assertEquals(
        "uforwarder-controller", controllerContainer.getEnvMap().get("UFORWARDER_PROFILE"));
  }

  @Test
  public void testSetupZkConnect() {
    String zkConnect = "127.0.0.1:2181";
    controllerContainer.withZookeeperConnect(zkConnect);
    Assertions.assertEquals(
        zkConnect, controllerContainer.getEnvMap().get("UFORWARDER_ZOOKEEPER_CONNECT"));
  }

  @Test
  public void testSetupMemoryLimit() {
    controllerContainer.withMaxMemoryMB(1024);
    Assertions.assertEquals(
        "1024", controllerContainer.getEnvMap().get("UFORWARDER_MEMORY_LIMIT_MB"));
  }

  @Test
  public void testwithKafkaBootstrapString() {
    controllerContainer.withKafkaBootstrapString("localhost:9001");
    Assertions.assertEquals(
        "localhost:9001", controllerContainer.getEnvMap().get("UFORWARDER_KAFKA_CONNECT"));
  }
}
