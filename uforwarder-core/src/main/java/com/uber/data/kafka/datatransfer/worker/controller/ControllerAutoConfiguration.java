package com.uber.data.kafka.datatransfer.worker.controller;

import com.google.common.net.HostAndPort;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.HostResolver;
import com.uber.data.kafka.datatransfer.common.ManagedChannelFactory;
import com.uber.data.kafka.datatransfer.common.StaticResolver;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@ConditionalOnProperty(
  prefix = "worker.controller",
  name = "enabled",
  havingValue = "true",
  matchIfMissing = true
)
@EnableConfigurationProperties({GrpcControllerConfiguration.class})
@Profile("data-transfer-worker")
public class ControllerAutoConfiguration {

  @Bean
  public <IN, IN_RESPONSE, OUT, OUT_RESPONSE> GrpcController grpcController(
      GrpcControllerConfiguration config,
      @Qualifier("coreInfra") CoreInfra infra,
      Node node,
      HostResolver masterResolver,
      PipelineManager pipelineManager,
      ManagedChannelFactory managedChannelFactory) {
    return new GrpcController(
        config, infra, node, masterResolver, pipelineManager, managedChannelFactory);
  }

  @Bean
  @ConditionalOnProperty(
    prefix = "worker.controller",
    name = "type",
    havingValue = "default",
    matchIfMissing = false
  )
  public ManagedChannelFactory managedChannelFactory() {
    return ManagedChannelFactory.DEFAULT_INSTANCE;
  }

  @Bean
  @ConditionalOnProperty(
    prefix = "worker.controller",
    name = "type",
    havingValue = "default",
    matchIfMissing = false
  )
  public HostResolver masterClientResolver(GrpcControllerConfiguration config) {
    return new StaticResolver(HostAndPort.fromString(config.getMasterHostPort()));
  }
}
