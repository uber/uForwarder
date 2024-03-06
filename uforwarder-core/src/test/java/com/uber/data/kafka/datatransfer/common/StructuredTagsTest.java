package com.uber.data.kafka.datatransfer.common;

import static com.uber.data.kafka.datatransfer.common.StructuredFields.HOST;
import static com.uber.data.kafka.datatransfer.common.StructuredFields.PORT;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class StructuredTagsTest extends FievelTestBase {

  // test get node
  @Test
  public void testGetNode() {
    Node node = Node.newBuilder().setHost("host").setPort(1234).build();
    CoreInfra infra = CoreInfra.builder().withNode(node).build();

    Map<String, String> tags = StructuredTags.builder().setNode(infra.getNode()).build();
    Assert.assertEquals("host", tags.get(HOST));
    Assert.assertEquals(Integer.toString(1234), tags.get(PORT));
  }
}
