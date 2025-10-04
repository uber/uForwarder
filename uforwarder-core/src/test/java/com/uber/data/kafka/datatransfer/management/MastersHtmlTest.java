package com.uber.data.kafka.datatransfer.management;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MastersHtmlTest {
  @Test
  public void testRead() throws Exception {
    Assertions.assertNotNull(new MastersHtml().read());
  }
}
