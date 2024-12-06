package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import java.util.Map;
import java.util.function.Function;

/**
 * JobPodPlacementProvider is the class to check whether a job or worker should satisfy pod
 * requirement.
 *
 * <p>We will apply the default number of virtual partitions to each pod to do rebalance. However,
 * if there is a pod defined in reservedPartitionCountForPods, the reserved number of partitions
 * will be used when rebalancing the jobs within that pod.
 *
 * <p>This class should be used by RpcJobColocatingRebalancer because it leverages the virtual
 * partition inside the rebalancer to colocate the jobs from different pods.
 */
public class JobPodPlacementProvider {
  private final Function<StoredJob, String> jobPodProvider;

  private final Function<StoredWorker, String> workerPodProvider;

  private final Map<String, Integer> reservedPartitionCountForPods;

  private final int defaultNumberOfPartitions;

  public JobPodPlacementProvider(
      Function<StoredJob, String> jobPodProvider,
      Function<StoredWorker, String> workerPodProvider,
      Map<String, Integer> reservedPartitionCountForPods,
      int defaultNumberOfVirtualPartitions) {
    this.jobPodProvider = jobPodProvider;
    this.workerPodProvider = workerPodProvider;
    this.reservedPartitionCountForPods = ImmutableMap.copyOf(reservedPartitionCountForPods);
    this.defaultNumberOfPartitions = defaultNumberOfVirtualPartitions;
  }

  /**
   * Gets the job pod
   *
   * @param job The job to get pod
   */
  public String getJobPod(StoredJob job) {
    return jobPodProvider.apply(job);
  }

  /**
   * Gets the worker pod
   *
   * @param worker The worker to get pod
   */
  public String getWorkerPod(StoredWorker worker) {
    return workerPodProvider.apply(worker);
  }

  /**
   * Gets number of partitions for a pod
   *
   * @param pod The pod to get number of partitions for
   */
  public int getNumberOfPartitionsForPod(String pod) {
    return reservedPartitionCountForPods.getOrDefault(pod, defaultNumberOfPartitions);
  }
}
