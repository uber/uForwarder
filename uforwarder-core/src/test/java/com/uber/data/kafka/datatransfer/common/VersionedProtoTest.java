package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.Node;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VersionedProtoTest {
  private Node item;
  private Node different;

  @BeforeEach
  public void setup() {
    item = Node.newBuilder().build();
    different = Node.newBuilder().setId(1).build();
  }

  @Test
  public void model() {
    Assertions.assertEquals(item, VersionedProto.from(item).model());
  }

  @Test
  public void version() {
    Assertions.assertEquals(2, VersionedProto.from(item, 2).version());
    Assertions.assertEquals(-1, VersionedProto.from(item).version());
  }

  @Test
  public void equals() {
    Versioned<Node> one = VersionedProto.from(item);
    Assertions.assertEquals(one, one);
    Assertions.assertEquals(one, VersionedProto.from(item));
    Assertions.assertNotEquals(one, VersionedProto.from(item, 2));
    Assertions.assertNotEquals(one, VersionedProto.from(different));
  }

  @Test
  public void testHashCode() {
    Assertions.assertEquals(
        VersionedProto.from(item).hashCode(), VersionedProto.from(item).hashCode());
    Assertions.assertNotEquals(
        VersionedProto.from(item).hashCode(), VersionedProto.from(different).hashCode());
  }
}
