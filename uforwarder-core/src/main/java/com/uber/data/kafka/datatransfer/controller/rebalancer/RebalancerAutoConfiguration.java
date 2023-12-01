package com.uber.data.kafka.datatransfer.controller.rebalancer;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("data-transfer-controller")
public class RebalancerAutoConfiguration {
  /**
   * Spring auto configuration for rebalancer that provides a default Rebalancer into spring DI
   * context.
   *
   * @implNote since we use {@code @ConditionalOnMissingBean} annotations, this default
   *     implementation will be used only if another Rebalancer implementation is not provided.
   */
  @ConditionalOnMissingBean
  @Bean
  public Rebalancer rebalancer() {
    return new Rebalancer() {};
  }
}
