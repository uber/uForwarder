package com.uber.data.kafka.datatransfer.management;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@EnableConfigurationProperties({ManagementServerConfiguration.class})
@Profile("data-transfer-controller")
public class MasterManagementAutoConfiguration {

  @Bean
  public MastersHtml mastersHtml() throws Exception {
    return new MastersHtml();
  }

  @Bean
  public MastersJson mastersJson(LeaderSelector leaderSelector) {
    return new MastersJson(leaderSelector);
  }

  @Bean
  public WorkersHtml workersHtml() throws Exception {
    return new WorkersHtml();
  }

  @Bean
  public WorkersJson workersJson(
      Store<String, StoredJobGroup> jobGroupStore,
      Store<Long, StoredWorker> workerStore,
      NodeUrlResolver workerUrlResolver)
      throws Exception {
    return new WorkersJson(workerStore, jobGroupStore, workerUrlResolver);
  }

  @Bean
  public JobsHtml jobsHtml() throws Exception {
    return new JobsHtml("masterJobs.html");
  }

  @Bean
  public MasterJobsJson jobsJson(
      Store<String, StoredJobGroup> jobGroupStore,
      Node node,
      ManagementServerConfiguration managementServerConfiguration,
      JsonFormat.TypeRegistry typeRegistry) {
    return new MasterJobsJson(
        jobGroupStore,
        node.getHost(),
        managementServerConfiguration.getDebugUrlFormat(),
        typeRegistry);
  }

  @Bean
  public NavJson navJson(Node node, @Value("${service.name}") String serviceName) {
    return new NavJson(serviceName, node.getHost(), "master");
  }

  @Bean
  public ScalarSnapshotJson scalarSnapshotJson(
      Scalar scalar, JsonFormat.TypeRegistry typeRegistry) {
    return new ScalarSnapshotJson(scalar, typeRegistry);
  }
}
