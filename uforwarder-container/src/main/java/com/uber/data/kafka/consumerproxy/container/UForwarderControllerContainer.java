package com.uber.data.kafka.consumerproxy.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;

/** Creates and starts uforwarder controller in docker container */
public class UForwarderControllerContainer
    extends UForwarderContainer<UForwarderControllerContainer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(UForwarderControllerContainer.class);

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
}
