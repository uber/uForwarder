package com.uber.data.kafka.datatransfer.management;

import com.google.protobuf.util.JsonFormat;

public abstract class AbstractJobsJson {

  protected final JsonFormat.Printer jsonPrinter;
  private final String hostName;
  private final String debugUrlFormat;

  public AbstractJobsJson(
      String hostName, String debugUrlFormat, JsonFormat.TypeRegistry typeRegistry) {
    this.hostName = hostName;
    this.debugUrlFormat = debugUrlFormat;
    this.jsonPrinter =
        JsonFormat.printer()
            .usingTypeRegistry(typeRegistry)
            .omittingInsignificantWhitespace()
            .includingDefaultValueFields();
  }

  public String getRpcDebugUrl() {
    return String.format(debugUrlFormat, hostName);
  }
}
