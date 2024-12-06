package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import java.util.List;
import java.util.Map;

/**
 * PodAwareRebalanceGroup is a data structure used for rebalancing among each pod. This is used in
 * RpcJobColocatingRebalancer
 */
public class PodAwareRebalanceGroup {
  private final String pod;
  private final int numberOfVirtualPartitions;
  private final Map<String, List<StoredJob>> groupIdToJobs;
  private final Map<Long, StoredWorker> workers;

  public PodAwareRebalanceGroup(
      String pod,
      int numberOfVirtualPartitions,
      Map<String, List<StoredJob>> groupIdToJobs,
      Map<Long, StoredWorker> workers) {
    this.pod = pod;
    this.numberOfVirtualPartitions = numberOfVirtualPartitions;
    this.groupIdToJobs = groupIdToJobs;
    this.workers = workers;
  }

  /** Gets the pod */
  public String getPod() {
    return pod;
  }

  /** Gets the number of virtual partitions */
  public int getNumberOfVirtualPartitions() {
    return numberOfVirtualPartitions;
  }

  /** Gets the group id to jobs map */
  public Map<String, List<StoredJob>> getGroupIdToJobs() {
    return groupIdToJobs;
  }

  /** Gets the workers */
  public Map<Long, StoredWorker> getWorkers() {
    return workers;
  }
}
