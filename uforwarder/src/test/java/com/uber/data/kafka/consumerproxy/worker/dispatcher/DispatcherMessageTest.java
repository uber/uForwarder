package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcRequest;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DispatcherMessageTest extends FievelTestBase {
  private static final String MUTTLEY_ROUTING_KEY = "muttley://routing-key";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static Headers HEADERS = new RecordHeaders().add("key", new byte[] {1});
  private static final String GROUP = "group";
  private static final String TOPIC = "topic";
  private static final int PARTITION = 1;
  private static final long OFFSET = 2;
  private static final String PHYSICAL_TOPIC = "topic";
  private static final String PHYSICAL_CLUSTER = "cluster";
  private static final int PHYSICAL_PARTITION = 1;
  private static final long PHYSICAL_OFFSET = 2;

  private static final long RETRY_COUNT = 3;
  private static final long DISPATCH_ATTEMPT = 4;
  private static final long TIMEOUT_COUNT = 5;

  private DispatcherMessage grpcDispatcherMessage;
  private GrpcRequest grpcRequest;

  private DispatcherMessage kafkaDispatcherMessage;
  private ProducerRecord<byte[], byte[]> producerRecord;
  private Job job;
  private MessageStub mockStub;

  @Before
  public void setup() {
    job =
        Job.newBuilder()
            .setJobId(0)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(GROUP)
                    .setTopic(TOPIC)
                    .setPartition(PARTITION)
                    .setCluster(PHYSICAL_CLUSTER)
                    .build())
            .build();
    mockStub = Mockito.mock(MessageStub.class);
    grpcDispatcherMessage =
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT);
    grpcRequest =
        new GrpcRequest(
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            HEADERS,
            VALUE.getBytes(),
            KEY.getBytes());
    kafkaDispatcherMessage =
        new DispatcherMessage(
            DispatcherMessage.Type.KAFKA,
            TOPIC,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT);
    producerRecord = new ProducerRecord<>(TOPIC, null, KEY.getBytes(), VALUE.getBytes(), HEADERS);
  }

  @Test
  public void testGetKafkaProducerRecord() {
    Assert.assertEquals(TOPIC, kafkaDispatcherMessage.getProducerRecord().topic());
    Assert.assertArrayEquals(KEY.getBytes(), kafkaDispatcherMessage.getProducerRecord().key());
    Assert.assertArrayEquals(VALUE.getBytes(), kafkaDispatcherMessage.getProducerRecord().value());
    Assert.assertArrayEquals(
        HEADERS.lastHeader("key").value(),
        kafkaDispatcherMessage.getProducerRecord().headers().lastHeader("key").value());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetKafkaProducerRecordWrongType() {
    grpcDispatcherMessage.getProducerRecord();
  }

  @Test
  public void testGetGrpcMessage() {
    Assert.assertEquals(grpcRequest, grpcDispatcherMessage.getGrpcMessage());
  }

  @Test
  public void testGetGrpcMessageNullKey() {
    GrpcRequest expected =
        new GrpcRequest(
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT,
            VALUE.getBytes(),
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            HEADERS);
    Assert.assertEquals(
        expected,
        new DispatcherMessage(
                DispatcherMessage.Type.GRPC,
                MUTTLEY_ROUTING_KEY,
                null,
                VALUE.getBytes(),
                HEADERS,
                GROUP,
                TOPIC,
                PARTITION,
                OFFSET,
                mockStub,
                PHYSICAL_TOPIC,
                PHYSICAL_CLUSTER,
                PHYSICAL_PARTITION,
                PHYSICAL_OFFSET,
                RETRY_COUNT,
                DISPATCH_ATTEMPT,
                TIMEOUT_COUNT)
            .getGrpcMessage());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetGrpcMessageWrongType() {
    kafkaDispatcherMessage.getGrpcMessage();
  }

  @Test
  public void testGetType() {
    Assert.assertEquals(DispatcherMessage.Type.GRPC, grpcDispatcherMessage.getType());
    Assert.assertEquals(DispatcherMessage.Type.KAFKA, kafkaDispatcherMessage.getType());
  }

  @Test
  public void testEquals() {
    Assert.assertEquals(grpcDispatcherMessage, grpcDispatcherMessage);
    Assert.assertEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));

    Assert.assertNotEquals(grpcDispatcherMessage, null);
    Assert.assertNotEquals(null, grpcDispatcherMessage);
    Assert.assertNotEquals(grpcDispatcherMessage, new Object());
    Assert.assertNotEquals(new Object(), grpcDispatcherMessage);
    // validate that equality fails for difference in any single field.
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.KAFKA,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY + "foo",
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            VALUE.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            KEY.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            new RecordHeaders(),
            TOPIC,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            TOPIC,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            GROUP,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION + 1,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET + 1,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT + 1,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT + 1,
            TIMEOUT_COUNT));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT + 1));
    Assert.assertNotEquals(
        grpcDispatcherMessage,
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            MUTTLEY_ROUTING_KEY,
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            GROUP,
            TOPIC,
            PARTITION,
            OFFSET,
            mockStub,
            PHYSICAL_TOPIC,
            PHYSICAL_CLUSTER + 1,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            RETRY_COUNT,
            DISPATCH_ATTEMPT,
            TIMEOUT_COUNT));
  }

  @Test
  public void testHashCode() {
    Assert.assertEquals(
        grpcDispatcherMessage.hashCode(),
        new DispatcherMessage(
                DispatcherMessage.Type.GRPC,
                MUTTLEY_ROUTING_KEY,
                KEY.getBytes(),
                VALUE.getBytes(),
                HEADERS,
                GROUP,
                TOPIC,
                PARTITION,
                OFFSET,
                mockStub,
                PHYSICAL_TOPIC,
                PHYSICAL_CLUSTER,
                PHYSICAL_PARTITION,
                PHYSICAL_OFFSET,
                RETRY_COUNT,
                DISPATCH_ATTEMPT,
                TIMEOUT_COUNT)
            .hashCode());
    Assert.assertNotEquals(grpcDispatcherMessage.hashCode(), kafkaDispatcherMessage.hashCode());
  }
}
