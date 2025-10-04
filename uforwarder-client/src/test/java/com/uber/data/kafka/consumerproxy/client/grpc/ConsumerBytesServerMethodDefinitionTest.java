package com.uber.data.kafka.consumerproxy.client.grpc;

import io.grpc.ServerMethodDefinition;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ConsumerBytesServerMethodDefinitionTest {
  @Test
  public void testOf() {
    ServerCalls.UnaryMethod unaryMethod = Mockito.mock(ServerCalls.UnaryMethod.class);
    ServerMethodDefinition serverMethodDefinition =
        ConsumerBytesServerMethodDefinition.of("consumerGroup", "topic", unaryMethod);
    Assertions.assertEquals(
        "kafka.consumerproxy.consumerGroup/topic",
        serverMethodDefinition.getMethodDescriptor().getFullMethodName());
  }
}
