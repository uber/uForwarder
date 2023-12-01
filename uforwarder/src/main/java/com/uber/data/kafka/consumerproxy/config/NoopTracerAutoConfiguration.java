package com.uber.data.kafka.consumerproxy.config;

import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import java.util.Objects;
import javax.annotation.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** Configuration for noop tracer */
@ConfigurationProperties(prefix = "tracer")
public class NoopTracerAutoConfiguration {
  @Nullable static Tracer INSTANCE;

  @Bean
  public Tracer tracer() {
    if (INSTANCE == null) {
      INSTANCE = NoopTracerFactory.create();
    }
    return Objects.requireNonNull(INSTANCE);
  }
}
