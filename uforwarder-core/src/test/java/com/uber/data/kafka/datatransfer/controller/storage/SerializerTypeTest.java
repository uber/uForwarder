package com.uber.data.kafka.datatransfer.controller.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.JsonSerializationFactory;
import com.uber.data.kafka.datatransfer.common.ProtoSerializationFactory;
import org.junit.jupiter.api.Test;

public class SerializerTypeTest {

  @Test
  public void testSerializerReturned() {
    StoredWorker storedWorker = StoredWorker.newBuilder().build();
    assertTrue(SerializerType.JSON.getSerializer(storedWorker) instanceof JsonSerializationFactory);
    assertTrue(
        SerializerType.PROTO.getSerializer(storedWorker) instanceof ProtoSerializationFactory);
  }
}
