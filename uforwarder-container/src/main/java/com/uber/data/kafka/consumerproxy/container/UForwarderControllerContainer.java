package com.uber.data.kafka.consumerproxy.container;

import org.testcontainers.utility.DockerImageName;

/** Creates and starts uforwarder controller in docker container */
public class UForwarderControllerContainer
    extends UForwarderContainer<UForwarderControllerContainer> {

  public UForwarderControllerContainer(DockerImageName dockerImageName) {
    super(dockerImageName);
    withEnv(ENV_UFORWARDER_PROFILE, PROFILE_CONTROLLER);
  }

  /**
   * Sets zookeeper connect string e.g "zookeeper01:2181"
   *
   * @param zookeeperConnect the zookeeper connect
   * @return the container
   */
  public UForwarderControllerContainer withZookeeperConnect(String zookeeperConnect) {
    withEnv(ENV_UFORWARDER_ZOOKEEPER_CONNECT, zookeeperConnect);
    return self();
  }

  /**
   * Gets host and grpc port of controller
   *
   * @return the address
   */
  public String getAddress() {
    return String.format("%s:%s", getHost(), getMappedPort(CONTROLLER_GRPC_PORT));
  }
}
