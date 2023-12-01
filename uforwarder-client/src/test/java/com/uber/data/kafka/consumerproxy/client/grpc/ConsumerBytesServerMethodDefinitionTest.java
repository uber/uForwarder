package com.uber.data.kafka.consumerproxy.client.grpc;

import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.ServerMethodDefinition;
import io.grpc.stub.ServerCalls;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ConsumerBytesServerMethodDefinitionTest extends FievelTestBase {
  @Test
  public void testOf() {
    ServerCalls.UnaryMethod unaryMethod = Mockito.mock(ServerCalls.UnaryMethod.class);
    ServerMethodDefinition serverMethodDefinition =
        ConsumerBytesServerMethodDefinition.of("consumerGroup", "topic", unaryMethod);
    Assert.assertEquals(
        "kafka.consumerproxy.consumerGroup/topic",
        serverMethodDefinition.getMethodDescriptor().getFullMethodName());
  }
}
