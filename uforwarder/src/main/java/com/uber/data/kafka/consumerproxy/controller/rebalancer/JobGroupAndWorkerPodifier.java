package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.rebalancer.JobPodPlacementProvider;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.m3.tally.Scope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JobGroupAndWorkerPodSelector is a class to group jobGroups and workers based on pod. */
public class JobGroupAndWorkerPodifier {
  private static final Logger logger = LoggerFactory.getLogger(JobGroupAndWorkerPodifier.class);

  private final JobPodPlacementProvider jobPodPlacementProvider;
  private final Scope scope;

  public JobGroupAndWorkerPodifier(JobPodPlacementProvider jobPodPlacementProvider, Scope scope) {
    this.jobPodPlacementProvider = jobPodPlacementProvider;
    this.scope = scope;
  }

  /**
   * Groups jobGroups and workers by pod, the result will be stored in a PodAwareRebalanceGroup
   *
   * @param jobGroupMap All the jobGroups
   * @param workerMap All the workers
   * @return List of PodAwareRebalanceGroup
   */
  List<PodAwareRebalanceGroup> podifyJobGroupsAndWorkers(
      final Map<String, RebalancingJobGroup> jobGroupMap, final Map<Long, StoredWorker> workerMap) {
    Map<String, Map<String, List<StoredJob>>> podToJobs = new HashMap<>();
    Map<String, Map<Long, StoredWorker>> podToWorkers = new HashMap<>();

    for (Map.Entry<String, RebalancingJobGroup> jobGroupEntry : jobGroupMap.entrySet()) {
      String jobGroupId = jobGroupEntry.getKey();
      for (StoredJob job : jobGroupEntry.getValue().getJobs().values()) {
        String pod = jobPodPlacementProvider.getJobPod(job);
        podToJobs.putIfAbsent(pod, new HashMap<>());
        podToJobs.get(pod).putIfAbsent(jobGroupId, new ArrayList<>());
        Objects.requireNonNull(podToJobs.get(pod).get(jobGroupId)).add(job);
      }
    }

    for (Map.Entry<Long, StoredWorker> workerEntry : workerMap.entrySet()) {
      String workerPod = jobPodPlacementProvider.getWorkerPod(workerEntry.getValue());
      podToWorkers.putIfAbsent(workerPod, new HashMap<>());
      Objects.requireNonNull(podToWorkers.get(workerPod))
          .put(workerEntry.getKey(), workerEntry.getValue());
    }

    List<PodAwareRebalanceGroup> allRebalanceGroups = new ArrayList<>();

    Map<String, List<StoredJob>> jobGroupsWithoutWorkersInPod = new HashMap<>();

    // find out jobs that don't have workers in the pod
    for (Map.Entry<String, Map<String, List<StoredJob>>> entry : podToJobs.entrySet()) {
      String pod = entry.getKey();
      if (!podToWorkers.containsKey(pod)) {
        logger.warn(
            "Pod doesn't have workers. Distributing to other pods", StructuredLogging.pod(pod));
        scope.tagged(ImmutableMap.of("pod", pod)).counter("pod.without.worker").inc(1);
        jobGroupsWithoutWorkersInPod.putAll(entry.getValue());
      }
    }

    // redistribute these jobs to other pods because they don't have workers in their pods
    if (!jobGroupsWithoutWorkersInPod.isEmpty()) {
      selectFallbackPod(jobGroupsWithoutWorkersInPod, podToWorkers, podToJobs, workerMap.size());
    }

    for (Map.Entry<String, Map<String, List<StoredJob>>> entry : podToJobs.entrySet()) {
      String pod = entry.getKey();
      if (!podToWorkers.containsKey(pod)) {
        continue;
      }

      Map<Long, StoredWorker> workers = podToWorkers.get(pod);
      allRebalanceGroups.add(
          new PodAwareRebalanceGroup(
              pod,
              jobPodPlacementProvider.getNumberOfPartitionsForPod(pod),
              entry.getValue(),
              workers));
    }

    return allRebalanceGroups;
  }

  /**
   * If a pod doesn't have available worker, move the job to other pods based on the ratio of
   * workers in each pod
   */
  private void selectFallbackPod(
      Map<String, List<StoredJob>> jobGroupsWithoutWorkersInPod,
      Map<String, Map<Long, StoredWorker>> podToWorkers,
      Map<String, Map<String, List<StoredJob>>> podToJobs,
      int totalNumberOfWorkers) {
    Preconditions.checkArgument(totalNumberOfWorkers > 0);
    List<Map.Entry<String, List<StoredJob>>> jobGroupsWithoutWorkersInPodList =
        new ArrayList<>(jobGroupsWithoutWorkersInPod.entrySet());
    int index = 0;
    for (Map.Entry<String, Map<Long, StoredWorker>> entry : podToWorkers.entrySet()) {
      String pod = entry.getKey();
      int numberOfJobGroups =
          (int)
              Math.round(
                  ((double) entry.getValue().size()
                      / totalNumberOfWorkers
                      * jobGroupsWithoutWorkersInPodList.size()));

      for (int cnt = 0; cnt < numberOfJobGroups; cnt++) {
        Map.Entry<String, List<StoredJob>> jobGroupEntry =
            jobGroupsWithoutWorkersInPodList.get(index++);
        Objects.requireNonNull(podToJobs.get(pod))
            .computeIfAbsent(jobGroupEntry.getKey(), jg -> new ArrayList<>());
        List<StoredJob> allJobs =
            Objects.requireNonNull(podToJobs.get(pod)).get(jobGroupEntry.getKey());
        Objects.requireNonNull(allJobs).addAll(jobGroupEntry.getValue());
      }
    }
  }
}
