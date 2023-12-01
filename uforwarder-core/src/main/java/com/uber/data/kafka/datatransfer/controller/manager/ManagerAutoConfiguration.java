package com.uber.data.kafka.datatransfer.controller.manager;

import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancerAutoConfiguration;
import com.uber.data.kafka.datatransfer.controller.rebalancer.ShadowRebalancerDelegate;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.datatransfer.controller.storage.StoreAutoConfiguration;
import com.uber.m3.tally.Scope;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

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
  prefix = "master.manager",
  name = "enabled",
  havingValue = "true",
  matchIfMissing = true
)
@Import({StoreAutoConfiguration.class, RebalancerAutoConfiguration.class})
@Profile("data-transfer-controller")
@EnableScheduling
public class ManagerAutoConfiguration {

  @Bean
  public JobManager jobManager(
      Rebalancer rebalancer,
      ShadowRebalancerDelegate shadowRebalancerDelegate,
      Scope scope,
      Store<String, StoredJobGroup> jobGroupStore,
      Store<Long, StoredJobStatus> jobStatusStore,
      Store<Long, StoredWorker> workerStore,
      LeaderSelector leaderSelector) {
    return new JobManager(
        scope,
        jobGroupStore,
        jobStatusStore,
        workerStore,
        rebalancer,
        shadowRebalancerDelegate,
        leaderSelector);
  }

  @Bean
  public WorkerManager workerManager(
      Scope scope, Store<Long, StoredWorker> workerStore, LeaderSelector leaderSelector)
      throws Exception {
    return new WorkerManager(scope, workerStore, leaderSelector);
  }
}
