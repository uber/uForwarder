package com.uber.data.kafka.datatransfer.management;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WorkersHtmlTest {
  @Test
  public void testRead() throws Exception {
    Assertions.assertNotNull(new WorkersHtml().read());
  }
}
