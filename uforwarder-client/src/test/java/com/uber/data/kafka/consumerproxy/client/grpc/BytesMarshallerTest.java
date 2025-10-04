package com.uber.data.kafka.consumerproxy.client.grpc;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BytesMarshallerTest {
  private ConsumerBytesServerMethodDefinition.BytesMarshaller marshaller;

  @BeforeEach
  public void setup() {
    marshaller = new ConsumerBytesServerMethodDefinition.BytesMarshaller();
  }

  @Test
  public void testMarshaller() {
    ByteString bytes = ByteString.copyFrom("data", StandardCharsets.UTF_8);
    Assertions.assertEquals(
        bytes,
        marshaller.parse(marshaller.stream(ByteString.copyFrom("data", StandardCharsets.UTF_8))));
  }
}
