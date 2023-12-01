package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class VersionedProtoTest extends FievelTestBase {
  private Node item;
  private Node different;

  @Before
  public void setup() {
    item = Node.newBuilder().build();
    different = Node.newBuilder().setId(1).build();
  }

  @Test
  public void model() {
    Assert.assertEquals(item, VersionedProto.from(item).model());
  }

  @Test
  public void version() {
    Assert.assertEquals(2, VersionedProto.from(item, 2).version());
    Assert.assertEquals(-1, VersionedProto.from(item).version());
  }

  @Test
  public void equals() {
    Versioned<Node> one = VersionedProto.from(item);
    Assert.assertEquals(one, one);
    Assert.assertEquals(one, VersionedProto.from(item));
    Assert.assertNotEquals(one, VersionedProto.from(item, 2));
    Assert.assertNotEquals(one, VersionedProto.from(different));
  }

  @Test
  public void testHashCode() {
    Assert.assertEquals(VersionedProto.from(item).hashCode(), VersionedProto.from(item).hashCode());
    Assert.assertNotEquals(
        VersionedProto.from(item).hashCode(), VersionedProto.from(different).hashCode());
  }
}
