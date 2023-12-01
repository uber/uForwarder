package com.uber.data.kafka.datatransfer.management;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.DebugNav;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "navJson")
public class NavJson {
  private final String host;
  private final String mode;
  private final String service;
  private final JsonFormat.Printer jsonPrinter;

  public NavJson(String service, String host, String mode) {
    this.service = service;
    this.host = host;
    this.mode = mode;
    this.jsonPrinter =
        JsonFormat.printer().omittingInsignificantWhitespace().includingDefaultValueFields();
  }

  @ReadOperation
  public String read() throws Exception {
    return jsonPrinter.print(
        DebugNav.newBuilder().setService(service).setHost(host).setRole(mode).build());
  }
}
