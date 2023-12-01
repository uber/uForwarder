package com.uber.data.kafka.datatransfer.management;

import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "masters")
public class MastersHtml extends AbstractHtml {
  MastersHtml() throws Exception {
    super("masters.html");
  }
}
