package com.uber.data.kafka.datatransfer.common;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimestampUtilsTest {

  @Test
  public void testCurrentTimeMilliseconds() {
    Assertions.assertNotEquals(0, TimestampUtils.currentTimeMilliseconds().getSeconds());
    Assertions.assertEquals(0, TimestampUtils.currentTimeMilliseconds().getNanos() % 1000);
    Assertions.assertTrue(TimestampUtils.currentTimeMilliseconds().getSeconds() >= 0);
    Assertions.assertTrue(TimestampUtils.currentTimeMilliseconds().getNanos() >= 0);
  }

  @Test
  public void serialize() {
    JsonSerializationFactory<Timestamp> jsonSerializationFactory =
        new JsonSerializationFactory<>(
            Timestamp.newBuilder().build(), JsonFormat.TypeRegistry.getEmptyTypeRegistry());
    jsonSerializationFactory.serialize(TimestampUtils.currentTimeMilliseconds());
  }
}
