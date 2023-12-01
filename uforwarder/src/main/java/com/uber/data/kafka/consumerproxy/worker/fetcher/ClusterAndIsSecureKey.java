package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.uber.data.kafka.datatransfer.Job;
import java.util.Objects;

/** ClusterAndIsSecureKey identifies a unique <cluster, isSecure> pair */
public class ClusterAndIsSecureKey {
  // The canonical cluster name
  private final String cluster;
  // A boolean indicating if the cluster should be a secure one, or not
  private final boolean isSecure;

  private ClusterAndIsSecureKey(String cluster, boolean isSecure) {
    this.cluster = cluster;
    this.isSecure = isSecure;
  }

  /**
   * Extracts ClusterAndIsSecureKey from a job
   *
   * @param job the job
   * @return the cluster and secure key
   */
  public static ClusterAndIsSecureKey of(Job job) {
    return new ClusterAndIsSecureKey(
        job.getKafkaConsumerTask().getCluster(),
        job.hasSecurityConfig() && job.getSecurityConfig().getIsSecure());
  }

  /**
   * Gets the cluster
   *
   * @return the cluster
   */
  public String getCluster() {
    return cluster;
  }

  /**
   * Gets is secure
   *
   * @return if is secure
   */
  public boolean isSecure() {
    return isSecure;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClusterAndIsSecureKey that = (ClusterAndIsSecureKey) o;
    return Objects.equals(cluster, that.cluster) && Objects.equals(isSecure, that.isSecure);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cluster, isSecure);
  }
}
