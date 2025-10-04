package com.uber.data.kafka.datatransfer.common;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ZKStringSerializerTest {
  public ZKStringSerializer serializer;

  @BeforeEach
  public void setup() {
    serializer = new ZKStringSerializer();
  }

  @Test
  public void testSerialize() {
    byte[] result = serializer.serialize(null);
    Assertions.assertNull(result);
    result = serializer.serialize("test");
    Assertions.assertArrayEquals("test".getBytes(StandardCharsets.UTF_8), result);
  }

  @Test
  public void testDeserialize() {
    Object result = serializer.deserialize(null);
    Assertions.assertNull(result);
    result = serializer.deserialize("test".getBytes(StandardCharsets.UTF_8));
    Assertions.assertEquals("test", result);
  }
}
