package com.uber.data.kafka.consumerproxy.common;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

/** log application start event when application started successfully. */
public class ApplicationReadyListener implements ApplicationListener<ApplicationReadyEvent> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationReadyListener.class);
  private final String appName;
  private final Logger logger;

  /**
   * Instantiates a new Application ready listener.
   *
   * @param builder the builder
   */
  private ApplicationReadyListener(Builder builder) {
    this.appName = builder.appName;
    this.logger = builder.logger;
  }

  @Override
  public void onApplicationEvent(ApplicationReadyEvent event) {
    logger.info("{} started successfully", appName);
  }

  /**
   * New builder builder.
   *
   * @return the builder
   */
  public static Builder newBuilder(String appName) {
    return new Builder(appName);
  }

  /** The type Builder. */
  public static class Builder {
    private final String appName;
    private Logger logger = LOGGER;

    private Builder(String appName) {
      this.appName = appName;
    }

    @VisibleForTesting
    Builder withLogger(Logger logger) {
      this.logger = logger;
      return this;
    }

    /**
     * Builds instance
     *
     * @return the application ready listener
     */
    public ApplicationReadyListener build() {
      return new ApplicationReadyListener(this);
    }
  }
}
