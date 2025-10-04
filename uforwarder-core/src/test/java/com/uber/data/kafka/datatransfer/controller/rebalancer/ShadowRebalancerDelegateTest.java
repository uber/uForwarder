package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.ScaleStatus;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ShadowRebalancerDelegateTest {
  private ShadowRebalancerDelegate shadowRebalancerDelegate;

  private Map<String, RebalancingJobGroup> jobGroupMap;

  private Map<Long, StoredWorker> workerMap;

  @BeforeEach
  public void setup() throws Exception {
    shadowRebalancerDelegate = new ShadowRebalancerDelegate() {};

    StoredJobGroup.Builder jobGroupBuilder = StoredJobGroup.newBuilder();
    jobGroupBuilder.setState(JobState.JOB_STATE_RUNNING);
    jobGroupBuilder.setScaleStatus(ScaleStatus.newBuilder().setScale(2.0).build());
    jobGroupBuilder.setJobGroup(
        JobGroup.newBuilder()
            .setFlowControl(
                FlowControl.newBuilder()
                    .setMessagesPerSec(4)
                    .setBytesPerSec(8)
                    .setMaxInflightMessages(2)
                    .build())
            .build());
    jobGroupBuilder.addJobs(
        StoredJob.newBuilder()
            .setWorkerId(1)
            .setState(JobState.JOB_STATE_RUNNING)
            .setJob(Job.newBuilder().setJobId(1).build())
            .build());
    jobGroupBuilder.addJobs(
        StoredJob.newBuilder()
            .setWorkerId(2)
            .setState(JobState.JOB_STATE_INVALID)
            .setJob(Job.newBuilder().setJobId(2).build())
            .build());
    jobGroupMap =
        ImmutableMap.of(
            "test_topic",
            RebalancingJobGroup.of(Versioned.from(jobGroupBuilder.build(), 1), ImmutableMap.of()));
    workerMap =
        ImmutableMap.of(
            2L,
            StoredWorker.newBuilder().setNode(Node.newBuilder().setId(2L).build()).build(),
            3L,
            StoredWorker.newBuilder().setNode(Node.newBuilder().setId(3L).build()).build());
  }

  @Test
  public void testComputeLoad() throws Exception {
    shadowRebalancerDelegate.computeLoad(jobGroupMap);
    // No-op
  }

  @Test
  public void testComputeJobConfiguration() throws Exception {
    shadowRebalancerDelegate.computeJobConfiguration(jobGroupMap, workerMap);

    // No-op
    jobGroupMap
        .entrySet()
        .forEach(
            entry -> {
              for (StoredJob job : entry.getValue().getJobs().values()) {
                Assertions.assertFalse(job.getJob().hasFlowControl());
              }
            });
  }

  @Test
  public void testComputeJobState() throws Exception {
    shadowRebalancerDelegate.computeJobState(jobGroupMap, workerMap);
    // No-op
    jobGroupMap
        .entrySet()
        .forEach(
            entry -> {
              for (StoredJob job : entry.getValue().getJobs().values()) {
                if (job.getWorkerId() == 1) {
                  Assertions.assertEquals(JobState.JOB_STATE_RUNNING, job.getState());
                } else if (job.getWorkerId() == 2) {
                  Assertions.assertEquals(JobState.JOB_STATE_INVALID, job.getState());
                } else {
                  Assertions.fail("unexpected worker id");
                }
              }
            });
  }

  @Test
  public void testComputeWorkerId() throws Exception {
    shadowRebalancerDelegate.computeWorkerId(jobGroupMap, workerMap);
    // No-op
    Set<Long> workerIdSet = new HashSet<>();
    workerIdSet.add(1L);
    workerIdSet.add(2L);
    jobGroupMap
        .entrySet()
        .forEach(
            entry -> {
              for (StoredJob job : entry.getValue().getJobs().values()) {
                workerIdSet.remove(job.getWorkerId());
              }
            });
    Assertions.assertTrue(workerIdSet.isEmpty());
  }

  @Test
  public void testRunShadowRebalancer() throws Exception {
    Assertions.assertFalse(shadowRebalancerDelegate.runShadowRebalancer());
  }
}
