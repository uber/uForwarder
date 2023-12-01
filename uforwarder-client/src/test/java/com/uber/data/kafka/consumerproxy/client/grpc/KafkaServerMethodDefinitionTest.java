package com.uber.data.kafka.consumerproxy.client.grpc;

import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.stub.ServerCalls;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class KafkaServerMethodDefinitionTest extends FievelTestBase {
  @Test
  public void testOf() {
    ServerCalls.UnaryMethod unaryMethod = Mockito.mock(ServerCalls.UnaryMethod.class);
    MethodDescriptor.Marshaller marshaller = Mockito.mock(MethodDescriptor.Marshaller.class);
    ServerMethodDefinition serverMethodDefinition =
        KafkaServerMethodDefinition.of("consumerGroup", "topic", unaryMethod, marshaller);
    Assert.assertEquals(
        "kafka.consumerproxy.consumerGroup/topic",
        serverMethodDefinition.getMethodDescriptor().getFullMethodName());
  }
}
