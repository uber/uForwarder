package com.uber.data.kafka.consumerproxy.client.grpc;

import com.google.protobuf.ByteString;
import com.uber.fievel.testing.base.FievelTestBase;
import java.nio.charset.StandardCharsets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BytesMarshallerTest extends FievelTestBase {
  private ConsumerBytesServerMethodDefinition.BytesMarshaller marshaller;

  @Before
  public void setup() {
    marshaller = new ConsumerBytesServerMethodDefinition.BytesMarshaller();
  }

  @Test
  public void testMarshaller() {
    ByteString bytes = ByteString.copyFrom("data", StandardCharsets.UTF_8);
    Assert.assertEquals(
        bytes,
        marshaller.parse(marshaller.stream(ByteString.copyFrom("data", StandardCharsets.UTF_8))));
  }
}
