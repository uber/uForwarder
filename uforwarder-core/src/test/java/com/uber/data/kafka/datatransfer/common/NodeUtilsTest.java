package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NodeUtilsTest extends FievelTestBase {
  private Node node;

  @Before
  public void setup() {
    node = Node.newBuilder().setHost("localhost").setPort(1234).build();
  }

  @Test
  public void getHostAndPortString() {
    Assert.assertEquals("localhost:1234", NodeUtils.getHostAndPortString(node));
  }

  @Test
  public void newNode() {
    Assert.assertEquals(node, NodeUtils.newNode("localhost:1234"));
  }
}
