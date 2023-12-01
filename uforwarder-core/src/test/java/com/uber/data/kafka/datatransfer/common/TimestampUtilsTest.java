package com.uber.data.kafka.datatransfer.common;

import com.google.protobuf.Timestamp;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TimestampUtilsTest extends FievelTestBase {

  @Test
  public void testCurrentTimeMilliseconds() {
    Assert.assertNotEquals(0, TimestampUtils.currentTimeMilliseconds().getSeconds());
    Assert.assertEquals(0, TimestampUtils.currentTimeMilliseconds().getNanos() % 1000);
    Assert.assertTrue(TimestampUtils.currentTimeMilliseconds().getSeconds() >= 0);
    Assert.assertTrue(TimestampUtils.currentTimeMilliseconds().getNanos() >= 0);
  }

  @Test
  public void serialize() {
    JsonSerializationFactory<Timestamp> jsonSerializationFactory =
        new JsonSerializationFactory<>(Timestamp.newBuilder().build());
    jsonSerializationFactory.serialize(TimestampUtils.currentTimeMilliseconds());
  }
}
