package com.uber.data.kafka.consumerproxy.client.grpc;

import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ConsumerResponseTest extends FievelTestBase {
  private StreamObserver streamObserver;

  @Before
  public void setup() {
    streamObserver = Mockito.mock(StreamObserver.class);
  }

  @Test
  public void testRetriableException() {
    ConsumerResponse.retriableException(streamObserver, Status.DATA_LOSS);
    ArgumentCaptor<StatusRuntimeException> captor =
        ArgumentCaptor.forClass(StatusRuntimeException.class);
    Mockito.verify(streamObserver, Mockito.times(1)).onError(captor.capture());
    StatusRuntimeException exception = captor.getValue();
    Assert.assertEquals(Status.DATA_LOSS.getCode(), exception.getStatus().getCode());
    Metadata metadata = exception.getTrailers();
    Assert.assertEquals(
        "Retry", metadata.get(Metadata.Key.of("kafka-action", Metadata.ASCII_STRING_MARSHALLER)));
  }

  @Test
  public void testRetriableExceptionWithoutStatus() {
    ConsumerResponse.retriableException(streamObserver);
    ArgumentCaptor<StatusRuntimeException> captor =
        ArgumentCaptor.forClass(StatusRuntimeException.class);
    Mockito.verify(streamObserver, Mockito.times(1)).onError(captor.capture());
    StatusRuntimeException exception = captor.getValue();
    Assert.assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), exception.getStatus().getCode());
  }

  @Test
  public void testNonRetriableException() {
    ConsumerResponse.nonRetriableException(streamObserver, Status.DATA_LOSS);
    ArgumentCaptor<StatusRuntimeException> captor =
        ArgumentCaptor.forClass(StatusRuntimeException.class);
    Mockito.verify(streamObserver, Mockito.times(1)).onError(captor.capture());
    StatusRuntimeException exception = captor.getValue();
    Assert.assertEquals(Status.DATA_LOSS.getCode(), exception.getStatus().getCode());
    Metadata metadata = exception.getTrailers();
    Assert.assertEquals(
        "Stash", metadata.get(Metadata.Key.of("kafka-action", Metadata.ASCII_STRING_MARSHALLER)));
  }

  @Test
  public void testNonRetriableExceptionWithoutStatus() {
    ConsumerResponse.nonRetriableException(streamObserver);
    ArgumentCaptor<StatusRuntimeException> captor =
        ArgumentCaptor.forClass(StatusRuntimeException.class);
    Mockito.verify(streamObserver, Mockito.times(1)).onError(captor.capture());
    StatusRuntimeException exception = captor.getValue();
    Assert.assertEquals(Status.FAILED_PRECONDITION.getCode(), exception.getStatus().getCode());
    Metadata metadata = exception.getTrailers();
  }

  @Test
  public void testDropMessageExceptionWithOKStatus() {
    ConsumerResponse.dropMessageException(streamObserver, Status.OK);
    ArgumentCaptor<StatusRuntimeException> captor =
        ArgumentCaptor.forClass(StatusRuntimeException.class);
    Mockito.verify(streamObserver, Mockito.times(1)).onError(captor.capture());
    StatusRuntimeException exception = captor.getValue();
    Assert.assertEquals(Status.FAILED_PRECONDITION.getCode(), exception.getStatus().getCode());
    Metadata metadata = exception.getTrailers();
    Assert.assertEquals(
        "Skip", metadata.get(Metadata.Key.of("kafka-action", Metadata.ASCII_STRING_MARSHALLER)));
  }

  @Test
  public void testDropMessageExceptionWithFailureStatus() {
    ConsumerResponse.dropMessageException(streamObserver, Status.RESOURCE_EXHAUSTED);
    ArgumentCaptor<StatusRuntimeException> captor =
        ArgumentCaptor.forClass(StatusRuntimeException.class);
    Mockito.verify(streamObserver, Mockito.times(1)).onError(captor.capture());
    StatusRuntimeException exception = captor.getValue();
    Assert.assertEquals(Status.RESOURCE_EXHAUSTED.getCode(), exception.getStatus().getCode());
    Metadata metadata = exception.getTrailers();
    Assert.assertEquals(
        "Skip", metadata.get(Metadata.Key.of("kafka-action", Metadata.ASCII_STRING_MARSHALLER)));
  }

  @Test
  public void testDropMessageExceptionWithoutStatus() {
    ConsumerResponse.dropMessageException(streamObserver);
    ArgumentCaptor<StatusRuntimeException> captor =
        ArgumentCaptor.forClass(StatusRuntimeException.class);
    Mockito.verify(streamObserver, Mockito.times(1)).onError(captor.capture());
    StatusRuntimeException exception = captor.getValue();
    Assert.assertEquals(Status.FAILED_PRECONDITION.getCode(), exception.getStatus().getCode());
    Metadata metadata = exception.getTrailers();
    Assert.assertEquals(
        "Skip", metadata.get(Metadata.Key.of("kafka-action", Metadata.ASCII_STRING_MARSHALLER)));
  }

  @Test
  public void testCommit() {
    ConsumerResponse.commit(streamObserver);
    Mockito.verify(streamObserver, Mockito.times(1)).onCompleted();
  }
}
