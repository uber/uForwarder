package com.uber.data.kafka.datatransfer.management;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for management UI */
@ConfigurationProperties(prefix = "management.server")
public class ManagementServerConfiguration {

  private String debugUrlFormat = "";

  /**
   * Gets the debug url format
   *
   * @return
   */
  public String getDebugUrlFormat() {
    return debugUrlFormat;
  }

  /**
   * Sets debug url format
   *
   * @param debugUrlFormat
   */
  public void setDebugUrlFormat(String debugUrlFormat) {
    this.debugUrlFormat = debugUrlFormat;
  }
}
