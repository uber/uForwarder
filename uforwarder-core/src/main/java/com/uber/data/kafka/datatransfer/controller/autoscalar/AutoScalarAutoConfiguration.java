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
  public ScaleStatusStore scaleStatusStore(AutoScalarConfiguration autoScalarConfiguration) {
    return new ScaleStatusStore(autoScalarConfiguration);
  }

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
  public ReactiveScaleWindowCalculator reactiveScaleWindowCalculator() {
    return new ReactiveScaleWindowCalculator();
  }

  @Bean
  public ReactiveScaleWindowManager scaleWindowManager(
      ScaleStatusStore scaleStatusStore,
      AutoScalarConfiguration autoScalarConfiguration,
      ReactiveScaleWindowCalculator reactiveScaleWindowCalculator) {
    return new ReactiveScaleWindowManager(
        scaleStatusStore,
        autoScalarConfiguration,
        Ticker.systemTicker(),
        reactiveScaleWindowCalculator);
  }

  @Bean
  public Scalar scalar(
      AutoScalarConfiguration autoScalarConfiguration,
      ScaleStatusStore scaleStatusStore,
      JobWorkloadSink jobWorkloadSink,
      ReactiveScaleWindowManager scaleWindowManager,
      Scope scope,
      LeaderSelector leaderSelector) {
    if (jobWorkloadSink instanceof JobWorkloadMonitor) {
      return new AutoScalar(
          autoScalarConfiguration,
          scaleStatusStore,
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
