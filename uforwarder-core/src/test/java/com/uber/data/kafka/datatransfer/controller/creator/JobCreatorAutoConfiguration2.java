package com.uber.data.kafka.datatransfer.controller.creator;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobCreatorAutoConfiguration2 {
  @Bean
  public JobCreator jobCreator() {
    return new SimpleJobCreator() {};
  }

  static class SimpleJobCreator implements JobCreator {}
  ;
}
