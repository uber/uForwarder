package com.uber.data.kafka.datatransfer.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.WorkerState;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Before;
import org.junit.Test;

public class ProtoSerializationFactoryTest extends FievelTestBase {
  private ProtoSerializationFactory<StoredWorker> serializationFactory;

  @Before
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

  @Test(expected = RuntimeException.class)
  public void testInvalidProtoBytes() {
    serializationFactory.deserialize(new byte[] {1, 2, 3, 4});
  }
}
