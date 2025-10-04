package com.uber.data.kafka.datatransfer.management;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AbstractHtmlTest {
  @Test
  public void testRead() throws Exception {
    AbstractHtml html = new SimpleHtml();
    Assertions.assertNotNull(html.read());
  }

  private static class SimpleHtml extends AbstractHtml {
    SimpleHtml() throws Exception {
      super("masters.html");
    }
  }
}
