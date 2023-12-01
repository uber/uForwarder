package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcherConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(
  prefix = "worker.fetcher.kafka",
  name = "enabled",
  havingValue = "true",
  matchIfMissing = false
)
@EnableConfigurationProperties(KafkaFetcherConfiguration.class)
public class KafkaFetcherAutoConfiguration {
  @Bean
  public KafkaFetcherFactory kafkaFetcherFactory(KafkaFetcherConfiguration config) {
    return new KafkaFetcherFactory(config);
  }
}
