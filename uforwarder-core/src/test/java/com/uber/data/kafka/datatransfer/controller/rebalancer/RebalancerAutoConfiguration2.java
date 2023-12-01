package com.uber.data.kafka.datatransfer.controller.rebalancer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RebalancerAutoConfiguration2 {
  @Bean
  public Rebalancer rebalancer() {
    return new SimpleRebalancer() {};
  }

  static class SimpleRebalancer implements Rebalancer {}
}
