package com.uber.data.kafka.datatransfer.controller.storage;

import static org.junit.Assert.assertTrue;

import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.JsonSerializationFactory;
import com.uber.data.kafka.datatransfer.common.ProtoSerializationFactory;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Test;

public class SerializerTypeTest extends FievelTestBase {

  @Test
  public void testSerializerReturned() {
    StoredWorker storedWorker = StoredWorker.newBuilder().build();
    assertTrue(SerializerType.JSON.getSerializer(storedWorker) instanceof JsonSerializationFactory);
    assertTrue(
        SerializerType.PROTO.getSerializer(storedWorker) instanceof ProtoSerializationFactory);
  }
}
