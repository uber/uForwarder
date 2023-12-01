package com.uber.data.kafka.clients.admin;

/**
 * MultiClusterAdminFactory is a common interface that returns a single cluster admin client, lazily
 * creating one if necessary.
 */
public interface MultiClusterAdminFactory {
  /**
   * Returns a single cluster Admin client for this clusterName.
   *
   * @param clusterName that the Admin client should be connected to.
   * @return Admin client that is connected to a single Kafka cluster.
   */
  Admin getAdmin(String clusterName);
}
