package com.uber.data.kafka.consumerproxy.container;

import org.testcontainers.utility.DockerImageName;

/** Creates and starts uforwarder worker in docker container */
public class UForwarderWorkerContainer extends UForwarderContainer<UForwarderWorkerContainer> {

  public UForwarderWorkerContainer(DockerImageName dockerImageName) {
    super(dockerImageName);
    withEnv(ENV_UFORWARDER_PROFILE, PROFILE_WORKER);
  }

  /**
   * Sets controller connect string e.g. "controller01:8087"
   *
   * @param controllerConnectString
   * @return the test container
   */
  public UForwarderWorkerContainer withController(String controllerConnectString) {
    withEnv(ENV_UFORWARDER_CONTROLLER_CONNECT, controllerConnectString);
    return self();
  }
}
