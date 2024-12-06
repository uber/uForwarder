package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.rebalancer.JobPodPlacementProvider;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Scope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Test;

public class JobGroupAndWorkerPodifierTest extends FievelTestBase {
  @Test
  public void test() throws Exception {
    Map<String, RebalancingJobGroup> jobGroupMap = new HashMap<>();

    List<StoredJob> allJobs = new ArrayList<>();

    // default pod
    for (int i = 0; i < 100; i++) {
      StoredJob storedJob =
          StoredJob.newBuilder()
              .setJobPod("default")
              .setJob(Job.newBuilder().setJobId(i).build())
              .build();
      allJobs.add(storedJob);
      StoredJobGroup storedJobGroup = StoredJobGroup.newBuilder().addAllJobs(allJobs).build();
      jobGroupMap.put(
          "defaultjobGroup" + i,
          RebalancingJobGroup.of(Versioned.from(storedJobGroup, 0), ImmutableMap.of()));
      allJobs.clear();
    }

    // pod1
    for (int i = 0; i < 20; i++) {
      StoredJob storedJob =
          StoredJob.newBuilder()
              .setJobPod("pod1")
              .setJob(Job.newBuilder().setJobId(i + 100).build())
              .build();
      allJobs.add(storedJob);
      StoredJobGroup storedJobGroup = StoredJobGroup.newBuilder().addAllJobs(allJobs).build();
      jobGroupMap.put(
          "pod1jobGroup" + i,
          RebalancingJobGroup.of(Versioned.from(storedJobGroup, 0), ImmutableMap.of()));
      allJobs.clear();
    }

    // pod2
    for (int i = 0; i < 20; i++) {
      StoredJob storedJob =
          StoredJob.newBuilder()
              .setJobPod("pod2")
              .setJob(Job.newBuilder().setJobId(i + 200).build())
              .build();
      allJobs.add(storedJob);
      StoredJobGroup storedJobGroup = StoredJobGroup.newBuilder().addAllJobs(allJobs).build();
      jobGroupMap.put(
          "pod2jobGroup" + i,
          RebalancingJobGroup.of(Versioned.from(storedJobGroup, 0), ImmutableMap.of()));
      allJobs.clear();
    }

    Map<Long, StoredWorker> workerMap = new HashMap<>();

    // default pod
    for (int i = 0; i < 50; i++) {
      workerMap.put(
          new Long(i),
          StoredWorker.newBuilder().setNode(Node.newBuilder().setHost("dca0-" + i)).build());
    }

    // pod1
    for (int i = 0; i < 25; i++) {
      workerMap.put(
          new Long(i + 50),
          StoredWorker.newBuilder().setNode(Node.newBuilder().setHost("dca1-" + i)).build());
    }

    Function<StoredJob, String> jobPod = job -> job.getJobPod();
    Function<StoredWorker, String> workerPod =
        storedWorker -> {
          if (storedWorker.getNode().getHost().startsWith("dca0")) {
            return "default";
          } else {
            return "pod1";
          }
        };

    JobPodPlacementProvider jobPodPlacementProvider =
        new JobPodPlacementProvider(jobPod, workerPod, ImmutableMap.of(), 8);
    Scope scope = mock(Scope.class);
    when(scope.tagged(anyMap())).thenReturn(scope);
    Counter counter = mock(Counter.class);
    when(scope.counter(any())).thenReturn(counter);
    JobGroupAndWorkerPodifier jobGroupAndWorkerPodifier =
        new JobGroupAndWorkerPodifier(jobPodPlacementProvider, scope);
    List<PodAwareRebalanceGroup> result =
        jobGroupAndWorkerPodifier.podifyJobGroupsAndWorkers(jobGroupMap, workerMap);

    Assert.assertEquals(result.size(), 2);
    int actualDefaultJobs = 0;
    int actualPod1Jobs = 0;
    int actualPod2Jobs = 0;
    for (int i = 0; i < 2; i++) {
      PodAwareRebalanceGroup podAwareRebalanceGroup = result.get(i);
      String pod = podAwareRebalanceGroup.getPod();
      Map<Long, StoredWorker> workers = podAwareRebalanceGroup.getWorkers();
      Map<String, List<StoredJob>> jobs = podAwareRebalanceGroup.getGroupIdToJobs();
      if (pod.equals("default")) {
        Assert.assertEquals(workers.size(), 50);
      } else if (pod.equals("pod1")) {
        Assert.assertEquals(workers.size(), 25);
      } else {
        Assert.fail("only has two pods");
      }

      int tempPod2Jobs = 0;
      for (String jobGroupId : jobs.keySet()) {
        List<StoredJob> allStoredobs = jobs.get(jobGroupId);
        for (StoredJob job : allStoredobs) {
          if (job.getJobPod().equals("default")) {
            actualDefaultJobs += 1;
          } else if (job.getJobPod().equals("pod1")) {
            actualPod1Jobs += 1;
          } else {
            actualPod2Jobs += 1;
            tempPod2Jobs += 1;
          }
        }
      }
      if (pod.equals("default")) {
        Assert.assertEquals(13, tempPod2Jobs);
      } else {
        Assert.assertEquals(7, tempPod2Jobs);
      }
    }
    Assert.assertEquals(actualDefaultJobs, 100);
    Assert.assertEquals(actualPod1Jobs, 20);
    Assert.assertEquals(actualPod2Jobs, 20);
  }
}
