package com.uber.data.kafka.datatransfer.management;

import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "jobs")
public class JobsHtml extends AbstractHtml {
  JobsHtml(String filename) throws Exception {
    super(filename);
  }
}
