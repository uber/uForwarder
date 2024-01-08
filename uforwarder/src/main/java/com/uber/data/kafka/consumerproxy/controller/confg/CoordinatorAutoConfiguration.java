package com.uber.data.kafka.consumerproxy.controller.confg;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.NodeUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.datatransfer.controller.config.ZookeeperConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

/** Configuration for curator leader selector */
@Profile("uforwarder-controller")
public class CoordinatorAutoConfiguration {

  static final String ZK_DATA_PATH = "/leader";
  static final char ZK_PATH_SEPARATOR = '/';

  @Bean
  public LeaderSelector leaderSelector(
      ZookeeperConfiguration zkConfiguration, Node node, CoreInfra coreInfra) throws Exception {
    if (zkConfiguration.isAutoCreateRootNode()) {
      String zkConnect = zkConfiguration.getZkConnection();
      int indexSeparator = zkConnect.indexOf(ZK_PATH_SEPARATOR);
      if (indexSeparator > 0 && indexSeparator < zkConnect.length() - 1) {
        // there is at least one layer of node in the path
        String address = zkConnect.substring(0, indexSeparator);
        String rootNode = zkConnect.substring(indexSeparator);
        ZkClient zkClient = new ZkClient(address);
        zkClient.createPersistent(rootNode, true);
      }
    }
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
