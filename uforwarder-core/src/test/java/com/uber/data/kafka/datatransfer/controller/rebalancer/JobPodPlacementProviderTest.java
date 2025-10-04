package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JobPodPlacementProviderTest {
  @Test
  public void test() throws Exception {
    JobPodPlacementProvider jobPodPlacementProvider =
        new JobPodPlacementProvider(
            job -> "", worker -> "", ImmutableMap.of("pod1", 1), ImmutableMap.of("pod1", 0.01), 2);
    Assertions.assertEquals("", jobPodPlacementProvider.getJobPod(StoredJob.newBuilder().build()));
    Assertions.assertEquals(
        "", jobPodPlacementProvider.getWorkerPod(StoredWorker.newBuilder().build()));
    Assertions.assertEquals(1, jobPodPlacementProvider.getNumberOfPartitionsForPod("pod1"));
    Assertions.assertTrue(
        jobPodPlacementProvider.getMaybeReservedFlowControlRatioForPods("pod1").isPresent());
    Assertions.assertEquals(
        jobPodPlacementProvider.getMaybeReservedFlowControlRatioForPods("pod1").get(),
        0.01,
        0.00001);
  }
}
