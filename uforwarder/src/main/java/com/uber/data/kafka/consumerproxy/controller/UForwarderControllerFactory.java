package com.uber.data.kafka.consumerproxy.controller;

import com.uber.data.kafka.consumerproxy.config.KafkaAdminClientConfiguration;
import com.uber.data.kafka.consumerproxy.config.NoopTracerAutoConfiguration;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.consumerproxy.config.SchedulerConfiguration;
import com.uber.data.kafka.consumerproxy.controller.confg.CoordinatorAutoConfiguration;
import com.uber.data.kafka.consumerproxy.controller.rebalancer.BatchRpcUriRebalancer;
import com.uber.data.kafka.consumerproxy.controller.rebalancer.HibernatingJobRebalancer;
import com.uber.data.kafka.consumerproxy.controller.rebalancer.RpcJobColocatingRebalancer;
import com.uber.data.kafka.consumerproxy.controller.rebalancer.ShadowRebalancerDelegateImpl;
import com.uber.data.kafka.consumerproxy.controller.rebalancer.StreamingRpcUriRebalancer;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.AdminClient;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import com.uber.data.kafka.datatransfer.common.KafkaPartitionExpansionWatcher;
import com.uber.data.kafka.datatransfer.common.MetricsConfiguration;
import com.uber.data.kafka.datatransfer.common.NodeAutoConfiguration;
import com.uber.data.kafka.datatransfer.controller.autoscalar.AutoScalarAutoConfiguration;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.data.kafka.datatransfer.controller.config.JobStatusStoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.config.WorkerStoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.creator.BatchJobCreator;
import com.uber.data.kafka.datatransfer.controller.creator.JobCreator;
import com.uber.data.kafka.datatransfer.controller.creator.StreamingJobCreator;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rpc.ControllerAdminService;
import com.uber.data.kafka.datatransfer.controller.rpc.ControllerWorkerService;
import com.uber.data.kafka.datatransfer.controller.rpc.RpcAutoConfiguration;
import com.uber.data.kafka.datatransfer.controller.storage.IdProvider;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.data.kafka.datatransfer.controller.storage.StoreAutoConfiguration;
import com.uber.data.kafka.datatransfer.management.MasterManagementAutoConfiguration;
import com.uber.m3.tally.Scope;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

/* This class contains the configuration that is used to start controller instance */
@Configuration
@EnableConfigurationProperties({
  RebalancerConfiguration.class,
  MetricsConfiguration.class,
  NoopTracerAutoConfiguration.class,
  KafkaAdminClientConfiguration.class,
})
@Import({
  AutoScalarAutoConfiguration.class,
  StoreAutoConfiguration.class,
  CoordinatorAutoConfiguration.class,
  JobStatusStoreConfiguration.JobStatusStoreConfigurationOverride.class,
  WorkerStoreConfiguration.WorkerStoreConfigurationOverride.class,
  RpcAutoConfiguration.class,
  SchedulerConfiguration.class,
  NodeAutoConfiguration.class,
  MasterManagementAutoConfiguration.class
})
@Profile("uforwarder-controller")
public class UForwarderControllerFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(UForwarderControllerFactory.class);

  @Bean
  public Rebalancer rebalancer(
      RebalancerConfiguration rebalancerConfiguration,
      DynamicConfiguration dynamicConfiguration,
      AdminClient.Builder adminBuilder,
      Scope scope,
      Scalar scalar)
      throws IOException {
    switch (rebalancerConfiguration.getMode()) {
      case "StreamingRpcUriRebalancer":
        return new StreamingRpcUriRebalancer(
            scope,
            rebalancerConfiguration,
            scalar,
            new HibernatingJobRebalancer(rebalancerConfiguration));
      case "BatchRpcUriRebalancer":
        return new BatchRpcUriRebalancer(
            scope,
            rebalancerConfiguration,
            scalar,
            new HibernatingJobRebalancer(rebalancerConfiguration),
            adminBuilder,
            dynamicConfiguration);
      default:
        return new Rebalancer() {};
    }
  }

  @Bean
  public JobCreator jobCreator(
      @Value("${master.jobCreator}") String jobCreatorMode,
      AdminClient.Builder adminBuilder,
      CoreInfra coreInfra) {
    switch (jobCreatorMode) {
      case "StreamingJobCreator":
        return new StreamingJobCreator(coreInfra.scope());
      case "BatchJobCreator":
        return new BatchJobCreator(adminBuilder, coreInfra);
      default:
        return new JobCreator() {};
    }
  }

  @Bean
  public AdminClient.Builder adminBuilder(KafkaAdminClientConfiguration configuration) {
    return AdminClient.newBuilder(configuration::getProperties);
  }

  @Bean
  public KafkaPartitionExpansionWatcher kafkaPartitionExpansionWatcher(
      CoreInfra coreInfra,
      Store<String, StoredJobGroup> jobGroupStore,
      IdProvider<Long, StoredJob> jobIdProvider,
      JobCreator jobCreator,
      AdminClient.Builder adminBuilder,
      LeaderSelector leaderSelector) {
    return new KafkaPartitionExpansionWatcher(
        coreInfra, jobGroupStore, jobIdProvider, jobCreator, adminBuilder, leaderSelector);
  }

  @Bean(name = "grpcPort")
  public int grpcPort(@Value("${grpc.port}") int port) {
    return port;
  }

  @Bean
  public GrpcServerRunner grpcServerRunner(
      @Qualifier("grpcPort") int port,
      ControllerAdminService controllerAdminService,
      ControllerWorkerService controllerWorkerService) {
    return new GrpcServerRunner(port, controllerAdminService, controllerWorkerService);
  }

  @Bean
  public ShadowRebalancerDelegateImpl shadowRebalancerDelegate(
      CoreInfra coreInfra, RebalancerConfiguration rebalancerConfiguration, Scalar scalar) {
    return new ShadowRebalancerDelegateImpl(
        new RpcJobColocatingRebalancer(
            coreInfra.scope(),
            rebalancerConfiguration,
            scalar,
            new HibernatingJobRebalancer(rebalancerConfiguration),
            true),
        rebalancerConfiguration.getShouldRunShadowRebalancer());
  }
}
