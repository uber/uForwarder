package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.when;

import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import com.uber.fievel.testing.base.FievelTestBase;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class GrpcRequestTest extends FievelTestBase {
  private static final String HEADER_TEST_TOPIC = "header-test-topic";
  private static final String HEADER_TEST_GROUP = "header-test-group";
  private GrpcRequest request;
  private GrpcRequest emptyRequest;
  private GrpcRequest retryTopicRequest;
  private GrpcRequest nullKeyRequest;
  private DynamicConfiguration dynamicConfiguration;
  private static final Headers emptyHeaders = new RecordHeaders();
  private MessageStub mockStub;

  @Before
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
            "key".getBytes(),
            0);
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
            "key".getBytes(),
            0);
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
            "key".getBytes(),
            0);
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
            emptyHeaders,
            0);
  }

  @Test
  public void metadataInterceptors() {
    Assert.assertEquals(8, request.metadataInterceptors().length);
  }

  @Test
  public void metadataInterceptorsForRetryTopic() {
    Assert.assertEquals(9, retryTopicRequest.metadataInterceptors().length);
  }

  @Test
  public void testPayload() {
    Assert.assertEquals("value", request.payload().toStringUtf8());
  }

  @Test
  public void testEmptyPayload() {
    Assert.assertEquals("", emptyRequest.payload().toStringUtf8());
  }

  @Test
  public void testGetConsumergroup() {
    Assert.assertEquals("group", request.getConsumergroup());
  }

  @Test
  public void testGetTopic() {
    Assert.assertEquals("topic", request.getTopic());
  }

  @Test
  public void testGetPartition() {
    Assert.assertEquals(0, request.getPartition());
  }

  @Test
  public void testGetOffset() {
    Assert.assertEquals(0, request.getOffset());
  }

  @Test
  public void testGetRetryCount() {
    Assert.assertEquals(0, request.getRetryCount());
  }

  @Test
  public void testGetDispatchAttempt() {
    Assert.assertEquals(0, request.getDispatchAttempt());
  }

  @Test
  public void testGetConsumerRecordTimestamp() {
    Assert.assertEquals(0, request.getConsumerRecordTimestamp());
  }

  @Test
  public void testGetPhysicalCluster() {
    Assert.assertEquals("cluster", request.getPhysicalCluster());
  }

  @Test
  public void testGetFuture() {
    Assert.assertFalse(request.getFuture().isDone());
  }

  @Test
  public void testEquals() {
    Assert.assertEquals(request, request);
    Assert.assertEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(request, null);
    Assert.assertNotEquals(null, request);
    Assert.assertNotEquals(request, new Object());
    Assert.assertNotEquals(new Object(), request);
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(
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
            "key".getBytes(),
            0));
    Assert.assertNotEquals(request, nullKeyRequest);
  }

  @Test
  public void testHashCode() {
    Assert.assertEquals(request.hashCode(), request.hashCode());
    Assert.assertNotEquals(request.hashCode(), nullKeyRequest.hashCode());
  }

  @Test
  public void testGetStub() {
    Assert.assertEquals(mockStub, request.getStub());
  }
}
