package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.when;

import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GrpcRequestTest {
  private static final String HEADER_TEST_TOPIC = "header-test-topic";
  private static final String HEADER_TEST_GROUP = "header-test-group";
  private GrpcRequest request;
  private GrpcRequest emptyRequest;
  private GrpcRequest retryTopicRequest;
  private GrpcRequest nullKeyRequest;
  private DynamicConfiguration dynamicConfiguration;
  private static final Headers emptyHeaders = new RecordHeaders();
  private MessageStub mockStub;

  @BeforeEach
  public void setup() {
    dynamicConfiguration = Mockito.mock(DynamicConfiguration.class);
    when(dynamicConfiguration.isHeaderAllowed(anyMap())).thenReturn(true);
    mockStub = Mockito.mock(MessageStub.class);
    request =
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes());
    emptyRequest =
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            null,
            "key".getBytes());
    retryTopicRequest =
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "physicaltopic",
            "physicalCluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes());
    nullKeyRequest =
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "value".getBytes(),
            "physicaltopic",
            "physicalCluster",
            0,
            0,
            emptyHeaders);
  }

  @Test
  public void metadataInterceptors() {
    Assertions.assertEquals(7, request.metadataInterceptors().length);
  }

  @Test
  public void metadataInterceptorsForRetryTopic() {
    Assertions.assertEquals(8, retryTopicRequest.metadataInterceptors().length);
  }

  @Test
  public void testPayload() {
    Assertions.assertEquals("value", request.payload().toStringUtf8());
  }

  @Test
  public void testEmptyPayload() {
    Assertions.assertEquals("", emptyRequest.payload().toStringUtf8());
  }

  @Test
  public void testGetConsumergroup() {
    Assertions.assertEquals("group", request.getConsumergroup());
  }

  @Test
  public void testGetTopic() {
    Assertions.assertEquals("topic", request.getTopic());
  }

  @Test
  public void testGetPartition() {
    Assertions.assertEquals(0, request.getPartition());
  }

  @Test
  public void testGetOffset() {
    Assertions.assertEquals(0, request.getOffset());
  }

  @Test
  public void testGetRetryCount() {
    Assertions.assertEquals(0, request.getRetryCount());
  }

  @Test
  public void testGetDispatchAttempt() {
    Assertions.assertEquals(0, request.getDispatchAttempt());
  }

  @Test
  public void testGetPhysicalCluster() {
    Assertions.assertEquals("cluster", request.getPhysicalCluster());
  }

  @Test
  public void testGetFuture() {
    Assertions.assertFalse(request.getFuture().isDone());
  }

  @Test
  public void testEquals() {
    Assertions.assertEquals(request, request);
    Assertions.assertEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(request, null);
    Assertions.assertNotEquals(null, request);
    Assertions.assertNotEquals(request, new Object());
    Assertions.assertNotEquals(new Object(), request);
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group" + 1,
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic" + 1,
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            1,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            1,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            1,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            1,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            1,
            "physicaltopic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            new RecordHeaders().add("key", "value".getBytes(StandardCharsets.UTF_8)),
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            Mockito.mock(MessageStub.class),
            0,
            0,
            0,
            "topic",
            "cluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(
        request,
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            mockStub,
            0,
            0,
            0,
            "topic",
            "physicalCluster",
            0,
            0,
            emptyHeaders,
            "value".getBytes(),
            "key".getBytes()));
    Assertions.assertNotEquals(request, nullKeyRequest);
  }

  @Test
  public void testHashCode() {
    Assertions.assertEquals(request.hashCode(), request.hashCode());
    Assertions.assertNotEquals(request.hashCode(), nullKeyRequest.hashCode());
  }

  @Test
  public void testGetStub() {
    Assertions.assertEquals(mockStub, request.getStub());
  }
}
