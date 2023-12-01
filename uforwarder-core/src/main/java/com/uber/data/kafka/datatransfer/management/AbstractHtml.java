package com.uber.data.kafka.datatransfer.management;

import com.google.common.base.Preconditions;
import com.google.common.io.CharStreams;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;

public abstract class AbstractHtml {
  private final String html;

  AbstractHtml(String filename) throws Exception {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(filename);
    Preconditions.checkNotNull(
        inputStream, String.format("failed to load resource %s from class loader", filename));
    html = CharStreams.toString(new InputStreamReader(inputStream));
  }

  @ReadOperation(produces = "text/html")
  public String read() {
    return html;
  }
}
