package com.uber.data.kafka.consumerproxy.config;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class RebalanceConfigurationTest extends FievelTestBase {
  @Test
  public void test() throws Exception {
    RebalancerConfiguration rebalancerConfiguration = new RebalancerConfiguration();
    rebalancerConfiguration.setPlacementWorkerScaleHardLimit(1.5);
    Assert.assertEquals(1.5, rebalancerConfiguration.getPlacementWorkerScaleHardLimit(), 0.0001);

    rebalancerConfiguration.setPlacementWorkerScaleHardLimit(0.9);
    Assert.assertEquals(1.0, rebalancerConfiguration.getPlacementWorkerScaleHardLimit(), 0.0001);
    rebalancerConfiguration.setWorkerToReduceRatio(0.1);
    Assert.assertEquals(0.1, rebalancerConfiguration.getWorkerToReduceRatio(), 0.0001);
    rebalancerConfiguration.setMaxJobNumberPerWorker(500);
    Assert.assertEquals(500, rebalancerConfiguration.getMaxJobNumberPerWorker());
    rebalancerConfiguration.setMessagesPerSecPerWorker(5000);
    Assert.assertEquals(5000, rebalancerConfiguration.getMessagesPerSecPerWorker());
    rebalancerConfiguration.setTargetSpareWorkerPercentage(15);
    Assert.assertEquals(15, rebalancerConfiguration.getTargetSpareWorkerPercentage());
  }
}
