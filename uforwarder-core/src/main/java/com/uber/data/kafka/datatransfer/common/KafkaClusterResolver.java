package com.uber.data.kafka.datatransfer.common;

/**
 * KafkaClusterResolver defines the behavior of how to resolve the bootstrap brokers for a cluster.
 * This class and all its subclasses should have at least one non-arg constructor to work with the
 * framework
 */
public class KafkaClusterResolver {
  /**
   * resolves bootstrap server default behavior is returning the provided input parameter
   *
   * @param cluster the cluster
   * @param bootstrapServers the bootstrap servers
   * @param isSecure the is secure
   * @return the bootstrap servers
   * @throws Exception the exception
   * @deprecated use getBootstrapServersByDNS instead
   */
  @Deprecated
  public String getBootstrapServers(String cluster, String bootstrapServers, boolean isSecure)
      throws Exception {
    return bootstrapServers;
  }

  /**
   * resolves bootstrap server default behavior is returning the provided input parameter
   *
   * @param cluster the cluster
   * @param bootstrapServers the bootstrap servers
   * @param isSecure the is secure
   * @return the bootstrap servers
   * @throws Exception the exception
   */
  public String getBootstrapServersByDNS(String cluster, String bootstrapServers, boolean isSecure)
      throws Exception {
    return bootstrapServers;
  }
}
