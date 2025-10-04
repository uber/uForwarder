package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.base.Ticker;
import com.uber.data.kafka.datatransfer.common.MetricsConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rpc.JobThroughputSink;
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
  public JobThroughputSink jobThroughputSink(
      AutoScalarConfiguration autoScalarConfiguration, Scope scope) {
    if (autoScalarConfiguration.isEnabled()) {
      return new JobThroughputMonitor(autoScalarConfiguration, Ticker.systemTicker(), scope);
    } else {
      return JobThroughputSink.NOOP;
    }
  }

  @Bean
  public Scalar scalar(
      AutoScalarConfiguration autoScalarConfiguration,
      JobThroughputSink jobThroughputSink,
      Scope scope,
      LeaderSelector leaderSelector) {
    if (jobThroughputSink instanceof JobThroughputMonitor) {
      return new AutoScalar(
          autoScalarConfiguration,
          (JobThroughputMonitor) jobThroughputSink,
          Ticker.systemTicker(),
          scope,
          leaderSelector);
    } else {
      return Scalar.DEFAULT;
    }
  }
}
