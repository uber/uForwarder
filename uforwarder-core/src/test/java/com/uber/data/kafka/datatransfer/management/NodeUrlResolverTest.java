package com.uber.data.kafka.datatransfer.management;

import com.uber.data.kafka.datatransfer.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NodeUrlResolverTest {
  @Test
  public void test() {
    Node node = Node.newBuilder().setHost("localhost").setPort(8000).build();
    NodeUrlResolver nodeUrlResolver = new NodeUrlResolver();
    Assertions.assertEquals("http://localhost:8000/jobs", nodeUrlResolver.resolveLink(node));
  }
}
