package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class NodeUtilsTest {
  private Node node;

  @BeforeEach
  public void setup() {
    node = Node.newBuilder().setHost("localhost").setPort(1234).build();
  }

  @Test
  public void getHostAndPortString() {
    Assertions.assertEquals("localhost:1234", NodeUtils.getHostAndPortString(node));
  }

  @Test
  public void newNode() {
    Assertions.assertEquals(node, NodeUtils.newNode("localhost:1234"));
  }
}
