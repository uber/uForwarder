package com.uber.data.kafka.datatransfer.common;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.WorkerState;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JsonSerializationFactoryTest extends FievelTestBase {
  private JsonSerializationFactory<StoredWorker> factory;

  @Before
  public void setup() {
    factory =
        new JsonSerializationFactory<>(
            StoredWorker.newBuilder().build(), JsonFormat.TypeRegistry.newBuilder().build());
  }

  @Test
  public void testSerializationAndDeserialization() throws Exception {
    // test that serialize/deserialize returns same object
    StoredWorker worker1 =
        StoredWorker.newBuilder()
            .setNode(Node.newBuilder().setHost("hostname1").setPort(1).setId(1).build())
            .setState(WorkerState.WORKER_STATE_WORKING)
            .build();
    byte[] workerBytes1 = factory.serialize(worker1);
    Assert.assertEquals(worker1, factory.deserialize(workerBytes1));

    // test that the prototype pattern for JsonSerializationFactory does not affect output
    // that is, the prototype is used for its types and no data is stored there
    // that could change the result of future calls.
    StoredWorker worker2 =
        StoredWorker.newBuilder()
            .setNode(Node.newBuilder().setHost("hostname2").setPort(2).setId(2).build())
            .setState(WorkerState.WORKER_STATE_REGISTERING)
            .build();
    byte[] workerBytes2 = factory.serialize(worker2);
    // workerBytes1 and workerBytes2 should be different since the data is different
    Assert.assertNotEquals(workerBytes1, workerBytes2);
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidJsonBytes() {
    factory.deserialize(new byte[] {1, 2, 3, 4});
  }

  @Test
  public void testSerializeAndDeserializeExtension() throws InvalidProtocolBufferException {
    JsonSerializationFactory<JobGroup> factory =
        new JsonSerializationFactory<>(
            JobGroup.newBuilder().build(),
            JsonFormat.TypeRegistry.newBuilder().add(StoredWorker.getDescriptor()).build());
    byte[] json =
        factory.serialize(
            JobGroup.newBuilder()
                .setExtension(
                    Any.pack(
                        StoredWorker.newBuilder()
                            .setState(WorkerState.WORKER_STATE_WORKING)
                            .build()))
                .build());
    JobGroup group = factory.deserialize(json);
    Assert.assertEquals(
        WorkerState.WORKER_STATE_WORKING,
        group.getExtension().unpack(StoredWorker.class).getState());
  }
}
