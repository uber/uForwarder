package com.uber.data.kafka.datatransfer.management;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class AbstractHtmlTest extends FievelTestBase {
  @Test
  public void testRead() throws Exception {
    AbstractHtml html = new SimpleHtml();
    Assert.assertNotNull(html.read());
  }

  private static class SimpleHtml extends AbstractHtml {
    SimpleHtml() throws Exception {
      super("masters.html");
    }
  }
}
