package com.uber.data.kafka.consumerproxy.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

/** This provides fine-grained control on all tasks created by @EnableScheduling and @Scheduled */
@Configuration
public class SchedulerConfiguration implements SchedulingConfigurer {

  @Bean(destroyMethod = "shutdown")
  public Executor taskExecutor() {
    return Executors.newScheduledThreadPool(
        10, new ThreadFactoryBuilder().setNameFormat("scheduled-thread-pool-%d").build());
  }

  @Override
  public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
    taskRegistrar.setScheduler(taskExecutor());
  }
}
