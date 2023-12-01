package com.uber.data.kafka.consumerproxy.container;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/** Uforwarder test container */
public abstract class UForwarderContainer<T extends UForwarderContainer<T>>
    extends GenericContainer<T> {
  public static final int CONTROLLER_GRPC_PORT = 8087;
  public static final int WORKER_GRPC_PORT = 8088;
  protected static final String PROFILE_CONTROLLER = "uforwarder-controller";
  protected static final String PROFILE_WORKER = "uforwarder-worker";
  protected static final String ENV_UFORWARDER_PROFILE = "UFORWARDER_PROFILE";
  protected static final String ENV_UFORWARDER_CONTROLLER_CONNECT = "UFORWARDER_CONTROLLER_CONNECT";
  protected static final String ENV_UFORWARDER_KAFKA_CONNECT = "UFORWARDER_KAFKA_CONNECT";
  protected static final String ENV_UFORWARDER_ZOOKEEPER_CONNECT = "UFORWARDER_ZOOKEEPER_CONNECT";
  protected static final String ENV_UFORWARDER_MEMORY_LIMIT_MB = "UFORWARDER_MEMORY_LIMIT_MB";
  private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("uforwarder");

  protected UForwarderContainer(final DockerImageName dockerImageName) {
    super(dockerImageName);
    dockerImageName.assertCompatibleWith(DEFAULT_IMAGE_NAME);
  }

  /**
   * Sets kafka bootstrap string e.g. "kafka01:9092"
   *
   * @param kafkaBootstrapString the kafka bootstrap string
   * @return the test container
   */
  public T withKafkaBootstrapString(String kafkaBootstrapString) {
    withEnv(ENV_UFORWARDER_KAFKA_CONNECT, kafkaBootstrapString);
    return self();
  }

  /**
   * Sets memory size limit in megabytes e.g. 1024
   *
   * @param memoryMB the memory mb
   * @return the container
   */
  public T withMaxMemoryMB(int memoryMB) {
    withEnv(ENV_UFORWARDER_MEMORY_LIMIT_MB, String.valueOf(memoryMB));
    return self();
  }
}
