package com.uber.data.kafka.consumerproxy.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RebalanceConfigurationTest {
  @Test
  public void test() throws Exception {
    RebalancerConfiguration rebalancerConfiguration = new RebalancerConfiguration();
    rebalancerConfiguration.setPlacementWorkerScaleHardLimit(1.5);
    Assertions.assertEquals(
        1.5, rebalancerConfiguration.getPlacementWorkerScaleHardLimit(), 0.0001);

    rebalancerConfiguration.setPlacementWorkerScaleHardLimit(0.9);
    Assertions.assertEquals(
        1.0, rebalancerConfiguration.getPlacementWorkerScaleHardLimit(), 0.0001);
    rebalancerConfiguration.setWorkerToReduceRatio(0.1);
    Assertions.assertEquals(0.1, rebalancerConfiguration.getWorkerToReduceRatio(), 0.0001);
    rebalancerConfiguration.setMaxJobNumberPerWorker(500);
    Assertions.assertEquals(500, rebalancerConfiguration.getMaxJobNumberPerWorker());
    rebalancerConfiguration.setMessagesPerSecPerWorker(5000);
    Assertions.assertEquals(5000, rebalancerConfiguration.getMessagesPerSecPerWorker());
    rebalancerConfiguration.setTargetSpareWorkerPercentage(15);
    Assertions.assertEquals(15, rebalancerConfiguration.getTargetSpareWorkerPercentage());
  }
}
