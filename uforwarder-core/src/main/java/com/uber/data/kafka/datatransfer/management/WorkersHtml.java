package com.uber.data.kafka.datatransfer.management;

import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "workers")
public class WorkersHtml extends AbstractHtml {
  WorkersHtml() throws Exception {
    super("workers.html");
  }
}
