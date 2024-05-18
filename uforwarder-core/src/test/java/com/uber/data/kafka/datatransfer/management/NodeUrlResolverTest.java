package com.uber.data.kafka.datatransfer.management;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class NodeUrlResolverTest extends FievelTestBase {
  @Test
  public void test() {
    Node node = Node.newBuilder().setHost("localhost").setPort(8000).build();
    NodeUrlResolver nodeUrlResolver = new NodeUrlResolver();
    Assert.assertEquals("http://localhost:8000/jobs", nodeUrlResolver.resolveLink(node));
  }
}
