package com.uber.data.kafka.consumerproxy.client.grpc;

import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class KafkaServerMethodDefinitionTest {
  @Test
  public void testOf() {
    ServerCalls.UnaryMethod unaryMethod = Mockito.mock(ServerCalls.UnaryMethod.class);
    MethodDescriptor.Marshaller marshaller = Mockito.mock(MethodDescriptor.Marshaller.class);
    ServerMethodDefinition serverMethodDefinition =
        KafkaServerMethodDefinition.of("consumerGroup", "topic", unaryMethod, marshaller);
    Assertions.assertEquals(
        "kafka.consumerproxy.consumerGroup/topic",
        serverMethodDefinition.getMethodDescriptor().getFullMethodName());
  }
}
