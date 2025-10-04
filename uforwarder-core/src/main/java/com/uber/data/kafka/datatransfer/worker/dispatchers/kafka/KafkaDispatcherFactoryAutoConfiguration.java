package com.uber.data.kafka.datatransfer.worker.dispatchers.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@ConditionalOnProperty(
    prefix = "worker.dispatcher.kafka",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = false)
@EnableConfigurationProperties(KafkaDispatcherConfiguration.class)
@EnableScheduling
public class KafkaDispatcherFactoryAutoConfiguration<K, V> {
  @Bean
  public KafkaDispatcherFactory<K, V> getKafkaDispatcherFactory(
      KafkaDispatcherConfiguration configuration) throws Exception {
    return new KafkaDispatcherFactory<>(configuration);
  }
}
