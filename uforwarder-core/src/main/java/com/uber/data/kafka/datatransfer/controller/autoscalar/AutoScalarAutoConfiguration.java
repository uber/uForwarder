package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.base.Ticker;
import com.uber.data.kafka.datatransfer.common.MetricsConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rpc.JobWorkloadSink;
import com.uber.m3.tally.Scope;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableConfigurationProperties({AutoScalarConfiguration.class})
@Profile("data-transfer-controller")
@Import({MetricsConfiguration.class})
@EnableScheduling
public class AutoScalarAutoConfiguration {

  @Bean
  public JobWorkloadSink jobWorkloadSink(
      AutoScalarConfiguration autoScalarConfiguration, Scope scope) {
    if (autoScalarConfiguration.isEnabled()) {
      return new JobWorkloadMonitor(autoScalarConfiguration, Ticker.systemTicker(), scope);
    } else {
      return JobWorkloadMonitor.NOOP;
    }
  }

  @Bean
  public ScaleWindowManager scaleWindowManager(AutoScalarConfiguration autoScalarConfiguration) {
    return new ScaleWindowManager(autoScalarConfiguration);
  }

  @Bean
  public Scalar scalar(
      AutoScalarConfiguration autoScalarConfiguration,
      JobWorkloadSink jobWorkloadSink,
      ScaleWindowManager scaleWindowManager,
      Scope scope,
      LeaderSelector leaderSelector) {
    if (jobWorkloadSink instanceof JobWorkloadMonitor) {
      return new AutoScalar(
          autoScalarConfiguration,
          (JobWorkloadMonitor) jobWorkloadSink,
          scaleWindowManager,
          Ticker.systemTicker(),
          scope,
          leaderSelector);
    } else {
      return Scalar.DEFAULT;
    }
  }
}
