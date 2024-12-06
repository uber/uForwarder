package com.uber.data.kafka.consumerproxy.worker;

import com.uber.data.kafka.consumerproxy.config.GrpcDispatcherConfiguration;
import com.uber.data.kafka.consumerproxy.config.NoopTracerAutoConfiguration;
import com.uber.data.kafka.consumerproxy.config.ProcessorConfiguration;
import com.uber.data.kafka.consumerproxy.config.SchedulerConfiguration;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcDispatcherFactory;
import com.uber.data.kafka.consumerproxy.worker.fetcher.KafkaFetcherAutoConfiguration;
import com.uber.data.kafka.consumerproxy.worker.fetcher.KafkaFetcherFactory;
import com.uber.data.kafka.consumerproxy.worker.filter.CompositeFilter;
import com.uber.data.kafka.consumerproxy.worker.filter.Filter;
import com.uber.data.kafka.consumerproxy.worker.filter.OriginalClusterFilter;
import com.uber.data.kafka.consumerproxy.worker.limiter.AdaptiveInflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.limiter.LongFixedInflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.limiter.VegasAdaptiveInflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageAckStatusManager;
import com.uber.data.kafka.consumerproxy.worker.processor.OutboundMessageLimiter;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorFactory;
import com.uber.data.kafka.consumerproxy.worker.processor.SimpleOutboundMessageLimiter;
import com.uber.data.kafka.consumerproxy.worker.processor.UnprocessedMessageManager;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.CoreInfraAutoConfiguration;
import com.uber.data.kafka.datatransfer.common.MetricsConfiguration;
import com.uber.data.kafka.datatransfer.common.NodeAutoConfiguration;
import com.uber.data.kafka.datatransfer.management.WorkerManagementAutoConfiguration;
import com.uber.data.kafka.datatransfer.worker.controller.ControllerAutoConfiguration;
import com.uber.data.kafka.datatransfer.worker.dispatchers.kafka.KafkaDispatcherFactory;
import com.uber.data.kafka.datatransfer.worker.dispatchers.kafka.KafkaDispatcherFactoryAutoConfiguration;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineFactory;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManagerAutoConfiguration;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineMetricPublisher;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

/* This class contains the configuration that is used to start worker instance */
@Configuration
@Profile("uforwarder-worker")
@EnableConfigurationProperties({
  GrpcDispatcherConfiguration.class,
  ProcessorConfiguration.class,
  NoopTracerAutoConfiguration.class
})
@Import({
  KafkaDispatcherFactoryAutoConfiguration.class,
  KafkaFetcherAutoConfiguration.class,
  PipelineManagerAutoConfiguration.class,
  CoreInfraAutoConfiguration.class,
  MetricsConfiguration.class,
  ControllerAutoConfiguration.class,
  SchedulerConfiguration.class,
  NodeAutoConfiguration.class,
  WorkerManagementAutoConfiguration.class
})
public class UForwarderWorkerFactory {

  @Bean
  public ProcessorFactory processorFactory(
      CoreInfra coreInfra,
      ProcessorConfiguration configuration,
      OutboundMessageLimiter.Builder outboundMessageLimiterBuilder,
      MessageAckStatusManager.Builder ackStatusManagerBuilder,
      UnprocessedMessageManager.Builder unprocessedManagerBuilder,
      Filter.Factory filterFactory) {
    return new ProcessorFactory(
        coreInfra,
        configuration,
        outboundMessageLimiterBuilder,
        ackStatusManagerBuilder,
        unprocessedManagerBuilder,
        filterFactory);
  }

  @Bean
  public Filter.Factory filterFactory(ProcessorConfiguration configuration) {
    List<Filter.Factory> filterFactories = new ArrayList<>();
    if (configuration.isClusterFilterEnabled()) {
      filterFactories.add(OriginalClusterFilter.newFactory());
    }
    return CompositeFilter.newFactory(filterFactories.toArray(new Filter.Factory[0]));
  }

  @Bean
  public MessageAckStatusManager.Builder messageAckStatusManagerBuilder(
      CoreInfra coreInfra, ProcessorConfiguration config) {
    return new MessageAckStatusManager.Builder(config.getMaxAckCommitSkew(), coreInfra);
  }

  @Bean
  public UnprocessedMessageManager.Builder unprocessedManagerBuilder(
      CoreInfra coreInfra,
      ProcessorConfiguration config,
      LongFixedInflightLimiter sharedByteSizeLimiter) {
    return new UnprocessedMessageManager.Builder(config, sharedByteSizeLimiter, coreInfra);
  }

  @Bean
  public LongFixedInflightLimiter sharedByteSizeLimiter(ProcessorConfiguration configuration) {
    return new LongFixedInflightLimiter(configuration.getSharedInboundCacheByteSize());
  }

  @Bean
  public GrpcDispatcherFactory grpcDispatcherFactory(
      GrpcDispatcherConfiguration config, CoreInfra coreInfra) {
    return new GrpcDispatcherFactory(config, coreInfra);
  }

  @Bean
  public PipelineFactory pipelineFactory(
      @Value("${service.name}") String serviceName,
      CoreInfra coreInfra,
      KafkaFetcherFactory kafkaFetcherFactory,
      ProcessorFactory processorFactory,
      GrpcDispatcherFactory grpcDispatcherFactory,
      KafkaDispatcherFactory<byte[], byte[]> kafkaDispatcherFactory) {
    return new PipelineFactoryImpl(
        serviceName,
        coreInfra,
        kafkaFetcherFactory,
        processorFactory,
        grpcDispatcherFactory,
        kafkaDispatcherFactory);
  }

  @Bean
  public PipelineMetricPublisher pipelineMetricPublisher(PipelineManager pipelineManager) {
    return new PipelineMetricPublisher(pipelineManager);
  }

  @Bean
  public AdaptiveInflightLimiter.Builder adaptiveInflightLimiterBuilder() {
    return VegasAdaptiveInflightLimiter.newBuilder();
  }

  @Bean
  public OutboundMessageLimiter.Builder outboundMessageLimiterBuilder(
      CoreInfra coreInfra,
      AdaptiveInflightLimiter.Builder adaptiveInflightLimiterBuilder,
      ProcessorConfiguration configuration) {
    return new SimpleOutboundMessageLimiter.Builder(
            coreInfra, adaptiveInflightLimiterBuilder, configuration.isExperimentalLimiterEnabled())
        .withMaxOutboundCacheCount(configuration.getMaxOutboundCacheCount());
  }
}
