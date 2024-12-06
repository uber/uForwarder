package com.uber.data.kafka.datatransfer.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class JobPodPlacementProviderTest extends FievelTestBase {
  @Test
  public void test() throws Exception {
    JobPodPlacementProvider jobPodPlacementProvider =
        new JobPodPlacementProvider(job -> "", worker -> "", ImmutableMap.of("pod1", 1), 2);
    Assert.assertEquals("", jobPodPlacementProvider.getJobPod(StoredJob.newBuilder().build()));
    Assert.assertEquals(
        "", jobPodPlacementProvider.getWorkerPod(StoredWorker.newBuilder().build()));
    Assert.assertEquals(1, jobPodPlacementProvider.getNumberOfPartitionsForPod("pod1"));
  }
}
