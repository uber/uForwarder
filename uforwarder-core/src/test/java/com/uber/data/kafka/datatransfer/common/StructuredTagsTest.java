package com.uber.data.kafka.datatransfer.common;

import static com.uber.data.kafka.datatransfer.common.StructuredFields.HOST;
import static com.uber.data.kafka.datatransfer.common.StructuredFields.PORT;

import com.uber.data.kafka.datatransfer.Node;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StructuredTagsTest {

  // test get node
  @Test
  public void testGetNode() {
    Node node = Node.newBuilder().setHost("host").setPort(1234).build();
    CoreInfra infra = CoreInfra.builder().withNode(node).build();

    Map<String, String> tags = StructuredTags.builder().setNode(infra.getNode()).build();
    Assertions.assertEquals("host", tags.get(HOST));
    Assertions.assertEquals(Integer.toString(1234), tags.get(PORT));
  }
}
