package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.m3.tally.Scope;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@Profile("data-transfer-worker")
@EnableScheduling
public class PipelineManagerAutoConfiguration {
  @ConditionalOnMissingBean
  @Bean
  public PipelineManager pipelineManager(PipelineFactory pipelineFactory, Scope scope, Node node) {
    return new PipelineManager(
        scope.tagged(
            ImmutableMap.of(
                StructuredFields.HOST,
                node.getHost() != null ? node.getHost() : "unknown",
                StructuredFields.PORT,
                node.getPort() != 0 ? String.valueOf(node.getPort()) : "unknown")),
        pipelineFactory);
  }
}
