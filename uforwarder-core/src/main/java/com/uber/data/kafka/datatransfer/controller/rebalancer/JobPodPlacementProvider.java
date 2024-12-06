package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * JobPodPlacementProvider is the data structure to check whether a job or worker should satisfy pod
 * requirement. This class will be used by RpcJobColocatingRebalancer which will assign the jobs
 * under each pod to different virtual partitions.
 *
 * <ul>
 *   <h2>Pod: a job or worker can be defined as belonging to a dedicated logical group(pod), and
 *   these jobs can only be processed by the workers in the same pod.
 *   <li>jobPodProvider is used to define the pod for each job.
 *   <li>workerPodProvider is used to define the pod for each worker
 * </ul>
 *
 * <ul>
 *   <h2>Virtual Partition: Inside each pod, the jobs could be divided into multiple virtual
 *   partitions for soft isolation.</h2>
 *   <li>reservedPartitionCountForPods is used to define number of virtual partitions for each pod.
 *   <li>reservedFlowControlRatioForPods is used to define the number of flow control ratio for each
 *       pod
 * </ul>
 */
public class JobPodPlacementProvider {
  /* Function to check which pod that a job belongs to */
  private final Function<StoredJob, String> jobPodProvider;

  /* Function to check which pod that a worker belongs to */
  private final Function<StoredWorker, String> workerPodProvider;

  /* map from pod(string) to number of partition(integer) to define how many virtual partitions should be used for the pod during job assignment */
  private final Map<String, Integer> reservedPartitionCountForPods;

  /* map from pod(string) to number of flow control ratio(double) to define the ratio of flow control for jobs under the pod */
  private final Map<String, Double> reservedFlowControlRatioForPods;

  /* default number of partitions used for job assignment */
  private final int defaultNumberOfPartitions;

  public JobPodPlacementProvider(
      Function<StoredJob, String> jobPodProvider,
      Function<StoredWorker, String> workerPodProvider,
      Map<String, Integer> reservedPartitionCountForPods,
      Map<String, Double> reservedFlowControlRatioForPods,
      int defaultNumberOfVirtualPartitions) {
    this.jobPodProvider = jobPodProvider;
    this.workerPodProvider = workerPodProvider;
    this.reservedPartitionCountForPods = ImmutableMap.copyOf(reservedPartitionCountForPods);
    this.reservedFlowControlRatioForPods = ImmutableMap.copyOf(reservedFlowControlRatioForPods);
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

  /**
   * Gets the flow control ratio for pods if any
   *
   * @param pod The pod to get flow control ratio
   * @return Optional value, Optional.empty if there is no reserved flow control ratio
   */
  public Optional<Double> getMaybeReservedFlowControlRatioForPods(String pod) {
    return Optional.ofNullable(reservedFlowControlRatioForPods.get(pod));
  }
}
