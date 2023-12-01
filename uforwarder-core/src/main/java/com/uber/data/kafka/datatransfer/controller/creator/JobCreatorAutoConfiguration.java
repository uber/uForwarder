package com.uber.data.kafka.datatransfer.controller.creator;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("data-transfer-controller")
public class JobCreatorAutoConfiguration {
  /**
   * Spring auto configuration for job creator that provides a default JobCreator into spring DI
   * context.
   *
   * @implNote since we use {@code @ConditionalOnMissingBean} annotations, this default
   *     implementation will be used only if another Rebalancer implementation is not provided.
   */
  @ConditionalOnMissingBean
  @Bean
  public JobCreator jobCreator() {
    return new JobCreator() {};
  }
}
