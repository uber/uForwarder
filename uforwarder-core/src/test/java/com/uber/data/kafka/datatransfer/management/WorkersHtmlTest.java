package com.uber.data.kafka.datatransfer.management;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class WorkersHtmlTest extends FievelTestBase {
  @Test
  public void testRead() throws Exception {
    Assert.assertNotNull(new WorkersHtml().read());
  }
}
