package com.uber.data.kafka.datatransfer.management;

import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "jobStatus")
public class JobStatusHtml {
  private final String JOBS_HTML;

  JobStatusHtml() throws Exception {
    InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("workerJobStatus.html");
    Preconditions.checkNotNull(inputStream, "failed to load resource jobs.html from class loader");
    JOBS_HTML = CharStreams.toString(new InputStreamReader(inputStream));
  }

  @ReadOperation(produces = "text/html")
  public String read() {
    return JOBS_HTML;
  }
}
