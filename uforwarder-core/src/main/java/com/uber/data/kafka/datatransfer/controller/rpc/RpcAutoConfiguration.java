package com.uber.data.kafka.datatransfer.controller.rpc;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.ReadStore;
import com.uber.data.kafka.datatransfer.controller.autoscalar.AutoScalarAutoConfiguration;
import com.uber.data.kafka.datatransfer.controller.autoscalar.AutoScalarConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.datatransfer.controller.storage.StoreAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

/**
 * This is a factory for objects in this package.
 *
 * <p>For each item to be constructed, there are two construction methods:
 *
 * <ul>
 *   <li>Spring friendly provider, named {@code getX}, that returns spring {@code Bean}. This is
 *       automatically used in spring framework
 *   <li>Static factory method, named {@code buildX}, that returns the object for use outside of
 *       spring framework.
 * </ul>
 */
@Configuration
@ConditionalOnProperty(
    prefix = "datatransfer.master.rpc.autoconfiguration",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@Import({StoreAutoConfiguration.class, AutoScalarAutoConfiguration.class})
@Profile("data-transfer-controller")
public class RpcAutoConfiguration {

  @Bean
  ControllerWorkerService controllerWorkerService(
      CoreInfra coreInfra,
      Node node,
      Store<Long, StoredWorker> workerStore,
      ReadStore<Long, StoredJob> jobStore,
      Store<Long, StoredJobStatus> jobStatusStore,
      LeaderSelector leaderSelector,
      JobThroughputSink jobThroughputSink)
      throws Exception {
    return new ControllerWorkerService(
        coreInfra, node, workerStore, jobStore, jobStatusStore, leaderSelector, jobThroughputSink);
  }

  @Bean
  public ControllerAdminService controllerAdminService(
      CoreInfra coreInfra,
      Store<String, StoredJobGroup> jobGroupStore,
      Store<Long, StoredJobStatus> jobStatusStore,
      Store<Long, StoredWorker> workerReadStore,
      AutoScalarConfiguration autoScalarConfiguration,
      LeaderSelector leaderSelector) {
    return new ControllerAdminService(
        coreInfra,
        jobGroupStore,
        jobStatusStore,
        workerReadStore,
        autoScalarConfiguration,
        leaderSelector);
  }
}
