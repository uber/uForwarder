package com.uber.data.kafka.consumerproxy.client.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import java.nio.charset.Charset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ConsumerMetadataTest {
  private static final String TENANCY_HEADER_KEY = "x-uber-tenancy";
  private static final String TENANCY_HEADER_VALUE = "uber/testing/kafka";
  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_GROUP = "test-group";
  private final Metadata metadata = Mockito.mock(Metadata.class);
  private ServerCallHandler<ByteString, Void> serverCallHandler =
      new ServerCallHandler<>() {
        @Override
        public ServerCall.Listener<ByteString> startCall(
            ServerCall<ByteString, Void> call, Metadata headers) {
          return new ServerCall.Listener<>() {
            @Override
            public void onMessage(ByteString message) {
              verifyMetadata();
              super.onMessage(message);
            }

            @Override
            public void onHalfClose() {
              verifyMetadata();
              super.onHalfClose();
            }

            @Override
            public void onCancel() {
              verifyMetadata();
              super.onCancel();
            }

            @Override
            public void onComplete() {
              verifyMetadata();
              super.onComplete();
            }

            @Override
            public void onReady() {
              verifyMetadata();
              super.onReady();
            }
          };
        }
      };

  @Test
  public void testGetHeaderWithDisallowedPrefix() {
    ConsumerMetadata.runWithMetadata(
        metadata,
        () -> {
          try {
            String headerValue = ConsumerMetadata.getHeader("kafka-topic");
            Assertions.fail("get kafka-prefix header should throw error");
          } catch (IllegalArgumentException exception) {
            Assertions.assertEquals(
                "disallowed header key with prefix `kafka` supplied", exception.getMessage());
          }
          Mockito.verifyNoInteractions(metadata);
        });
  }

  @Test
  public void testGetHeaderSuccess() {
    ConsumerMetadata.runWithMetadata(
        metadata,
        () -> {
          Mockito.when(
                  metadata.get(
                      Metadata.Key.of(TENANCY_HEADER_KEY, Metadata.ASCII_STRING_MARSHALLER)))
              .thenReturn(TENANCY_HEADER_VALUE);

          try {
            String tenancy = ConsumerMetadata.getHeader(TENANCY_HEADER_KEY);
            Assertions.assertEquals(TENANCY_HEADER_VALUE, tenancy);
          } catch (IllegalArgumentException exception) {
            Assertions.fail("no exception should be thrown");
          }
        });
  }

  @Test
  public void testInterceptCall() {
    Mockito.when(metadata.get(Metadata.Key.of("kafka-topic", Metadata.ASCII_STRING_MARSHALLER)))
        .thenReturn(TEST_TOPIC);
    Mockito.when(
            metadata.get(Metadata.Key.of("kafka-consumergroup", Metadata.ASCII_STRING_MARSHALLER)))
        .thenReturn(TEST_GROUP);
    ServerCall.Listener<ByteString> intercepted =
        ConsumerMetadata.serverInterceptor()
            .interceptCall(Mockito.mock(ServerCall.class), metadata, serverCallHandler);
    intercepted.onMessage(ByteString.copyFrom("testData", Charset.defaultCharset()));
    intercepted.onComplete();
    intercepted.onHalfClose();
    intercepted.onCancel();
    intercepted.onReady();
  }

  private void verifyMetadata() {
    Assertions.assertEquals(TEST_TOPIC, ConsumerMetadata.getTopic());
    Assertions.assertEquals(TEST_GROUP, ConsumerMetadata.getConsumerGroup());
    Assertions.assertEquals(-1, ConsumerMetadata.getPartition());
    Assertions.assertEquals(-1L, ConsumerMetadata.getRetryCount());
  }
}
