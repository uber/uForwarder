package com.uber.data.kafka.datatransfer.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.WorkerState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProtoSerializationFactoryTest {
  private ProtoSerializationFactory<StoredWorker> serializationFactory;

  @BeforeEach
  public void setup() {
    serializationFactory = new ProtoSerializationFactory<>(StoredWorker.newBuilder().build());
  }

  @Test
  public void testSerializationAndDeserialization() {
    // test that serialize/deserialize returns same object
    StoredWorker workerOne =
        StoredWorker.newBuilder()
            .setNode(Node.newBuilder().setHost("hostOne").setPort(1).setId(1).build())
            .setState(WorkerState.WORKER_STATE_WORKING)
            .build();
    byte[] workerBytesOne = serializationFactory.serialize(workerOne);
    assertEquals(workerOne, serializationFactory.deserialize(workerBytesOne));

    StoredWorker workerSecond =
        StoredWorker.newBuilder()
            .setNode(Node.newBuilder().setHost("hostSecond").setPort(2).setId(2).build())
            .setState(WorkerState.WORKER_STATE_REGISTERING)
            .build();
    byte[] workerBytesTwo = serializationFactory.serialize(workerSecond);
    // workerBytesOne and workerBytesTwo should be different since the data is different
    assertNotEquals(workerBytesOne, workerBytesTwo);
  }

  @Test
  public void testInvalidProtoBytes() {
    assertThrows(
        RuntimeException.class, () -> serializationFactory.deserialize(new byte[] {1, 2, 3, 4}));
  }
}
