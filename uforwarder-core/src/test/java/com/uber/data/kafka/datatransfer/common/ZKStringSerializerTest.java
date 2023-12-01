package com.uber.data.kafka.datatransfer.common;

import com.uber.fievel.testing.base.FievelTestBase;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ZKStringSerializerTest extends FievelTestBase {
  public ZKStringSerializer serializer;

  @Before
  public void setup() {
    serializer = new ZKStringSerializer();
  }

  @Test
  public void testSerialize() {
    byte[] result = serializer.serialize(null);
    Assert.assertNull(result);
    result = serializer.serialize("test");
    Assert.assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void testDeserialize() {
    Object result = serializer.deserialize(null);
    Assert.assertNull(result);
    result = serializer.deserialize("test".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("test", result);
  }
}
