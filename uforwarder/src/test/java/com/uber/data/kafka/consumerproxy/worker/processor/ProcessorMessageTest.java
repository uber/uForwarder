package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.protobuf.ByteString;
import com.uber.data.kafka.consumer.DLQMetadata;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherMessage;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.ResqConfig;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.fievel.testing.base.FievelTestBase;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tag;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ProcessorMessageTest extends FievelTestBase {
  private static String KEY = "key";
  private static String VALUE = "value";
  private static Headers HEADERS =
      new RecordHeaders() {
        {
          {
            add("key", "value".getBytes(StandardCharsets.UTF_8));
          }
        }
      };
  private static String ORIGINAL_TOPIC = "topic";

  private static String ORIGINAL_CLUSTER = "kafka-lossless-dca";
  private static String ORIGINAL_GROUP = "group";
  private static int ORIGINAL_PARTITION = 0;
  private static long ORIGINAL_OFFSET = 0;

  private static long ORIGINAL_TIMESTAMP = 1000;
  private static String PHYSICAL_TOPIC = "topic";

  private static int PHYSICAL_PARTITION = 0;
  private static long PHYSICAL_OFFSET = 0;

  private static long PHYSICAL_TIMESTAMP = 1000;
  private static String DLQ_TOPIC = "topic__group__dlq";
  private static String DLQ_CLUSTER = "kafka-dlq-dca";
  private static int DLQ_PARTITION = 1;
  private static long DLQ_OFFSET = 1;
  private static long DLQ_TIMESTAMP = 2000;
  private static Job job = Job.newBuilder().build();
  private static Optional<Span> SPAN = Optional.empty();
  private static MessageStub stub = new MessageStub();
  private ProcessorMessage nonDLQMessage;
  private Job nonDLQJob;
  private ProcessorMessage dlqMessage;
  private Job dlqJob;
  private DLQMetadata dlqMetadata;
  protected CoreInfra infra;

  @Before
  public void setup() {
    Tracer tracer = new MockTracer();
    infra = CoreInfra.builder().withTracer(tracer).build();
    nonDLQMessage =
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            PHYSICAL_TOPIC,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            PHYSICAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub);
    nonDLQJob =
        Job.newBuilder()
            .setJobId(0)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(ORIGINAL_GROUP)
                    .setTopic(ORIGINAL_TOPIC)
                    .setPartition(ORIGINAL_PARTITION)
                    .build())
            .build();
    dlqMetadata =
        DLQMetadata.newBuilder()
            .setData(ByteString.copyFrom(KEY.getBytes()))
            .setTopic(ORIGINAL_TOPIC)
            .setPartition(ORIGINAL_PARTITION)
            .setOffset(ORIGINAL_OFFSET)
            .setTimestampNs(ORIGINAL_TIMESTAMP)
            .setRetryCount(1)
            .setTimeoutCount(0)
            .build();
    dlqJob =
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(ORIGINAL_GROUP)
                    .setTopic(DLQ_TOPIC)
                    .setPartition(DLQ_PARTITION)
                    .build())
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().setDlqTopic(DLQ_TOPIC).build())
            .build();
    dlqMessage =
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            DLQ_TOPIC,
            DLQ_CLUSTER,
            DLQ_PARTITION,
            DLQ_OFFSET,
            DLQ_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            1,
            0,
            SPAN,
            infra,
            stub);
    // Mockito.reset(contextManager);
  }

  @Test
  public void testCreationForDLQ() throws Exception {
    ProcessorMessage otherNonDLQMessage =
        ProcessorMessage.of(
            new ConsumerRecord<>(
                ORIGINAL_TOPIC,
                ORIGINAL_PARTITION,
                ORIGINAL_OFFSET,
                ORIGINAL_TIMESTAMP,
                TimestampType.CREATE_TIME,
                -1L,
                -1,
                -1,
                KEY.getBytes(),
                VALUE.getBytes()),
            nonDLQJob,
            infra,
            stub);
    Assert.assertEquals(nonDLQMessage, otherNonDLQMessage);

    ProcessorMessage otherDLQMessage =
        ProcessorMessage.of(
            new ConsumerRecord<>(
                DLQ_TOPIC,
                DLQ_PARTITION,
                DLQ_OFFSET,
                DLQ_TIMESTAMP,
                TimestampType.CREATE_TIME,
                -1L,
                -1,
                -1,
                dlqMetadata.toByteArray(),
                VALUE.getBytes()),
            dlqJob,
            infra,
            stub);
    Assert.assertEquals(dlqMessage, otherDLQMessage);
  }

  @Test
  public void testCreationForRetryQueue() throws Exception {
    String retryTopic = "topic__group__retry";
    int retryPartition = 2;
    long retryOffset = 2;
    long retryTimestamp = 2000;
    ProcessorMessage retryMessage =
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            retryTopic,
            DLQ_CLUSTER,
            retryPartition,
            retryOffset,
            retryTimestamp,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            1,
            0,
            SPAN,
            infra,
            stub);
    Job retryJob =
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(ORIGINAL_GROUP)
                    .setTopic(retryTopic)
                    .setPartition(retryPartition)
                    .build())
            .setRetryConfig(
                RetryConfig.newBuilder()
                    .setRetryEnabled(true)
                    .addRetryQueues(RetryQueue.newBuilder().setRetryQueueTopic(retryTopic).build())
                    .build())
            .build();
    ProcessorMessage otherRetryMessage =
        ProcessorMessage.of(
            new ConsumerRecord<>(
                retryTopic,
                retryPartition,
                retryOffset,
                retryTimestamp,
                TimestampType.CREATE_TIME,
                -1L,
                -1,
                -1,
                dlqMetadata.toByteArray(),
                VALUE.getBytes()),
            retryJob,
            infra,
            stub);
    Assert.assertEquals(retryMessage, otherRetryMessage);
  }

  @Test
  public void testCreationForResq() throws Exception {
    String resqTopic = "topic__group__resq";
    int resqPartition = 2;
    long resqOffset = 2;
    long resqTimestamp = 2000;
    ProcessorMessage resqMessage =
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            resqTopic,
            ORIGINAL_CLUSTER,
            resqPartition,
            resqOffset,
            resqTimestamp,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            1,
            0,
            SPAN,
            infra,
            stub);
    Job resqJob =
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(ORIGINAL_GROUP)
                    .setCluster(ORIGINAL_CLUSTER)
                    .setTopic(resqTopic)
                    .setPartition(resqPartition)
                    .build())
            .setResqConfig(ResqConfig.newBuilder().setResqTopic(resqTopic).build())
            .build();
    ProcessorMessage otherResqMessage =
        ProcessorMessage.of(
            new ConsumerRecord<>(
                resqTopic,
                resqPartition,
                resqOffset,
                resqTimestamp,
                TimestampType.CREATE_TIME,
                -1L,
                -1,
                -1,
                dlqMetadata.toByteArray(),
                VALUE.getBytes()),
            resqJob,
            infra,
            stub);
    Assert.assertEquals(resqMessage, otherResqMessage);
  }

  @Test
  public void testGetValueByteSize() {
    Assert.assertEquals(VALUE.getBytes().length, nonDLQMessage.getValueByteSize());
    nonDLQMessage =
        new ProcessorMessage(
            KEY.getBytes(),
            null,
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub);
    Assert.assertEquals(0, nonDLQMessage.getValueByteSize());
  }

  @Test
  public void incrementAndGetDispatchAttemptCount() {
    Assert.assertEquals(0, nonDLQMessage.getRetryCount());
    Assert.assertEquals(0, nonDLQMessage.getDispatchAttempt());
    // For non DLQ message only dispatch attempt count should be increased.
    nonDLQMessage.increaseAttemptCount();
    Assert.assertEquals(1, nonDLQMessage.getDispatchAttempt());
  }

  @Test
  public void incrementAndGetTimeoutCount() {
    Assert.assertEquals(0, nonDLQMessage.getTimeoutCount());
    nonDLQMessage.increaseTimeoutCount();
    Assert.assertEquals(1, nonDLQMessage.getTimeoutCount());
  }

  @Test
  public void testEquals() {
    Assert.assertEquals(nonDLQMessage, nonDLQMessage);
    ProcessorMessage processorMessage =
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub);
    Assert.assertEquals(nonDLQMessage, processorMessage);
    Assert.assertNotEquals(nonDLQMessage, dlqMessage);
    Assert.assertNotEquals(null, nonDLQMessage);
    Assert.assertNotEquals(nonDLQMessage, null);
    Assert.assertNotEquals(new Object(), nonDLQMessage);
    Assert.assertNotEquals(nonDLQMessage, new Object());

    // validate any single field difference results in not equal
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            VALUE.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            KEY.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_GROUP,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION + 1,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET + 1,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP + 1,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_TOPIC,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_GROUP,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION + 1,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET + 1,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP + 1,
            0,
            0,
            SPAN,
            infra,
            stub));
    Assert.assertNotEquals(
        nonDLQMessage,
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            1,
            0,
            SPAN,
            infra,
            stub));
    // For non DLQ message only dispatch attempt count should be increased.
    nonDLQMessage.increaseAttemptCount();
    Assert.assertNotEquals(nonDLQMessage, processorMessage);
  }

  @Test
  public void testHashCode() {
    Assert.assertEquals(
        nonDLQMessage.hashCode(),
        new ProcessorMessage(
                KEY.getBytes(),
                VALUE.getBytes(),
                HEADERS,
                ORIGINAL_TOPIC,
                ORIGINAL_CLUSTER,
                ORIGINAL_PARTITION,
                ORIGINAL_OFFSET,
                ORIGINAL_TIMESTAMP,
                ORIGINAL_GROUP,
                ORIGINAL_TOPIC,
                ORIGINAL_PARTITION,
                ORIGINAL_OFFSET,
                ORIGINAL_TIMESTAMP,
                0,
                0,
                SPAN,
                infra,
                stub)
            .hashCode());
    Assert.assertNotEquals(nonDLQMessage.hashCode(), dlqMessage.hashCode());
  }

  @Test
  public void testGetGrpcDispatcherMessage() {
    Assert.assertEquals(
        new DispatcherMessage(
            DispatcherMessage.Type.GRPC,
            "muttley://routing-key",
            KEY.getBytes(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            nonDLQMessage.getStub(),
            PHYSICAL_TOPIC,
            ORIGINAL_CLUSTER,
            PHYSICAL_PARTITION,
            PHYSICAL_OFFSET,
            0,
            0,
            0),
        nonDLQMessage.getGrpcDispatcherMessage("muttley://routing-key"));
  }

  @Test
  public void testGetKafkaDispatcherMessage() {
    Assert.assertEquals(
        new DispatcherMessage(
            DispatcherMessage.Type.KAFKA,
            DLQ_TOPIC,
            DLQMetadata.newBuilder()
                .setData(ByteString.copyFrom(KEY.getBytes()))
                .setTopic(ORIGINAL_TOPIC)
                .setPartition(ORIGINAL_PARTITION)
                .setOffset(ORIGINAL_OFFSET)
                .setTimestampNs(ORIGINAL_TIMESTAMP)
                .setRetryCount(1)
                .setTimeoutCount(0)
                .build()
                .toByteArray(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            dlqMessage.getStub(),
            "topic__group__dlq",
            DLQ_CLUSTER,
            1,
            1,
            1,
            0,
            0),
        dlqMessage.getKafkaDispatcherMessage(DLQ_TOPIC));

    dlqMessage =
        new ProcessorMessage(
            null,
            VALUE.getBytes(),
            HEADERS,
            DLQ_TOPIC,
            DLQ_CLUSTER,
            DLQ_PARTITION,
            DLQ_OFFSET,
            DLQ_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            1,
            1,
            SPAN,
            infra,
            stub);
    Assert.assertEquals(
        new DispatcherMessage(
            DispatcherMessage.Type.KAFKA,
            DLQ_TOPIC,
            DLQMetadata.newBuilder()
                .setTopic(ORIGINAL_TOPIC)
                .setPartition(ORIGINAL_PARTITION)
                .setOffset(ORIGINAL_OFFSET)
                .setTimestampNs(ORIGINAL_TIMESTAMP)
                .setRetryCount(1)
                .setTimeoutCount(1)
                .build()
                .toByteArray(),
            VALUE.getBytes(),
            HEADERS,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            dlqMessage.getStub(),
            "topic__group__dlq",
            DLQ_CLUSTER,
            1,
            1,
            1,
            0,
            1),
        dlqMessage.getKafkaDispatcherMessage(DLQ_TOPIC));
  }

  @Test
  public void testTracerLifecycleWithoutTracer() throws Exception {
    dlqMessage.close(null, null);
  }

  @Test
  public void testTracerLifecycleWithTracerAndNullParent() throws Exception {
    infra.tracer().activateSpan(dlqMessage.getSpan());
    dlqMessage.close(null, null);
  }

  @Test
  public void testTracerLifecycleWithTracerAndNonNullParent() throws Exception {
    Headers headers = new RecordHeaders();
    headers.add("key", new byte[] {1});
    ProcessorMessage message =
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            headers,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub);

    // Use mockito instead of mock span so that we mock exception.
    Tracer tracer = Mockito.mock(Tracer.class);
    Span parentSpan = Mockito.mock(Span.class);
    Tracer.SpanBuilder spanBuilder = Mockito.mock(Tracer.SpanBuilder.class);
    Span span = Mockito.mock(Span.class);
    Scope scope = Mockito.mock(Scope.class);

    Mockito.doReturn(parentSpan).when(tracer).activeSpan();
    Mockito.doReturn(spanBuilder).when(tracer).buildSpan(Mockito.anyString());
    Mockito.doReturn(spanBuilder).when(spanBuilder).asChildOf(Mockito.any(Span.class));
    Mockito.doReturn(spanBuilder)
        .when(spanBuilder)
        .withTag(Mockito.anyString(), Mockito.anyString());
    Mockito.doReturn(spanBuilder).when(spanBuilder).withTag(Mockito.anyString(), Mockito.anyInt());
    Mockito.doReturn(spanBuilder).when(spanBuilder).withTag(Mockito.anyString(), Mockito.anyLong());
    Mockito.doReturn(spanBuilder)
        .when(spanBuilder)
        .withTag(Mockito.any(Tag.class), Mockito.anyString());
    Mockito.doReturn(span).when(spanBuilder).start();
    Mockito.doReturn(scope).when(tracer).activateSpan(Mockito.any());

    tracer.activateSpan(dlqMessage.getSpan());
    message.close(null, null);
  }

  @Test
  public void testShouldDispatch() {
    Assert.assertTrue(nonDLQMessage.shouldDispatch());
    nonDLQMessage.setShouldDispatch(false);
    Assert.assertFalse(nonDLQMessage.shouldDispatch());
  }

  @Test
  public void testGetHeaders() {
    Headers headers = nonDLQMessage.getHeaders();
    Assert.assertEquals(1, headers.toArray().length);
  }

  @Test
  public void testGetLogicalTimestamp() {
    long logicalTimestamp = nonDLQMessage.getLogicalTimestamp();
    Assert.assertEquals(1000L, logicalTimestamp);
    logicalTimestamp = dlqMessage.getLogicalTimestamp();
    Assert.assertEquals(1000L, logicalTimestamp);
  }

  @Test
  public void testOffsetToCommit() {
    Assert.assertEquals(-1, nonDLQMessage.getOffsetToCommit());
    nonDLQMessage.setOffsetToCommit(100);
    Assert.assertEquals(100, nonDLQMessage.getOffsetToCommit());
  }

  @Test
  public void testGetProducerCluster() {
    Assert.assertEquals("", nonDLQMessage.getProducerCluster());

    Headers headers = new RecordHeaders();
    headers.add("original_cluster", "clustername".getBytes(StandardCharsets.UTF_8));
    ProcessorMessage nonNullHeader =
        new ProcessorMessage(
            KEY.getBytes(),
            VALUE.getBytes(),
            headers,
            ORIGINAL_TOPIC,
            ORIGINAL_CLUSTER,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            ORIGINAL_GROUP,
            ORIGINAL_TOPIC,
            ORIGINAL_PARTITION,
            ORIGINAL_OFFSET,
            ORIGINAL_TIMESTAMP,
            0,
            0,
            SPAN,
            infra,
            stub);
    Assert.assertEquals("clustername", nonNullHeader.getProducerCluster());
  }
}
