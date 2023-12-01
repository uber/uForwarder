package com.uber.data.kafka.consumerproxy.controller.confg;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.NodeUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.datatransfer.controller.config.ZookeeperConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Configuration for curator leader selector */
@Profile("uforwarder-controller")
public class CoordinatorAutoConfiguration {

  static final String ZK_DATA_PATH = "/leader";

  @Bean
  public LeaderSelector leaderSelector(
      ZookeeperConfiguration zkConfiguration, Node node, CoreInfra coreInfra) throws Exception {
    return LeaderSelector.of(
        zkConfiguration.getZkConnection(),
        ZK_DATA_PATH,
        NodeUtils.getHostAndPortString(node),
        coreInfra.tagged(
            ImmutableMap.of(
                StructuredFields.HOST,
                node.getHost(),
                StructuredFields.PORT,
                String.valueOf(node.getPort()))));
  }
}
