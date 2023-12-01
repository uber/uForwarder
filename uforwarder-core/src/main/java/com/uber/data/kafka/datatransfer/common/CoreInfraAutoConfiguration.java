package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.common.context.ContextManager;
import com.uber.m3.tally.Scope;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CoreInfraAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(DynamicConfiguration.class)
  public DynamicConfiguration defaultDynamicConfiguration() {
    return DynamicConfiguration.DEFAULT;
  }

  @Bean
  @ConditionalOnMissingBean(ContextManager.class)
  public ContextManager defaultContextManager() {
    return ContextManager.NOOP;
  }

  @Bean
  @ConditionalOnMissingBean(Tracer.class)
  public Tracer defaultTracer() {
    return NoopTracerFactory.create();
  }

  @Bean
  @ConditionalOnMissingBean(Placement.class)
  public Placement defaultPlacement() {
    return Placement.DEFAULT;
  }

  @Bean
  public CoreInfra coreInfra(
      Scope scope,
      Tracer tracer,
      ContextManager contextManager,
      DynamicConfiguration dynamicConfiguration,
      Placement placement) {
    return CoreInfra.builder()
        .withScope(scope)
        .withTracer(tracer)
        .withContextManager(contextManager)
        .withDynamicConfiguration(dynamicConfiguration)
        .withPlacement(placement)
        .build();
  }
}
