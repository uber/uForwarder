package com.uber.data.kafka.datatransfer.common;

import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.tally.m3.M3Reporter;
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

  private static final String METRICS_REPORTER_STATSD = "statsd";
  private static final String METRICS_REPORTER_M3 = "m3";

  // The metrics reporter to use, currently only M3 and statsd is supported now
  // see https://github.com/uber-java/tally for more details
  private String metricsReporter = METRICS_REPORTER_M3;

  public String getMetricsReporter() {
    return metricsReporter;
  }

  public void setMetricsReporter(String metricsReporter) {
    this.metricsReporter = metricsReporter;
  }

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
      StatsReporter statsReporter = null;
      if (metricsReporter.equals(METRICS_REPORTER_STATSD)) {
        StatsDClient statsd = new NonBlockingStatsDClientBuilder()
                .prefix(METRICS_REPORTER_STATSD)
                .hostname("localhost")
                .port(8125)
                .build();
        statsReporter = new StatsdReporter(statsd);
      }
      INSTANCE =
          new RootScopeBuilder()
              .reporter(statsReporter)
              .tags(new ImmutableMap.Builder<String, String>().build())
              .reportEvery(Duration.ofSeconds(tallyPublishIntervalSec));
    }
    return Objects.requireNonNull(INSTANCE);
  }
}
