package com.uber.data.kafka.datatransfer.common;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.statsd.StatsdReporter;
import com.uber.m3.util.Duration;
import com.uber.m3.util.ImmutableMap;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.inject.Singleton;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** Configuration for m3 metrics */
@ConfigurationProperties(prefix = "metrics")
public class MetricsConfiguration {
  @Nullable static Scope INSTANCE;

  @Bean
  @Singleton
  @ConditionalOnProperty(
    prefix = "metrics.rootScope",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true
  )
  @ConditionalOnMissingBean
  public Scope rootScope(@Value("${tally.publish.interval.sec:5}") int tallyPublishIntervalSec) {
    if (INSTANCE == null) {
      StatsDClient statsd = new NonBlockingStatsDClientBuilder()
              .prefix("kforwarder")
              .hostname("localhost")
              .port(8125)
              .build();
      INSTANCE =
          new RootScopeBuilder().reporter(new StatsdReporter(statsd))
              .tags(new ImmutableMap.Builder<String, String>().build())
              .reportEvery(Duration.ofSeconds(tallyPublishIntervalSec));
    }
    return Objects.requireNonNull(INSTANCE);
  }
}
