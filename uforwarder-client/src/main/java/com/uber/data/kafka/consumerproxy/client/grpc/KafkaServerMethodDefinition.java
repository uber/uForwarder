package com.uber.data.kafka.consumerproxy.client.grpc;

import com.google.protobuf.Empty;
import io.grpc.Internal;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;

/**
 * KafkaServerMethodDefinition contains methods that wrap creation of generic Kafka-Grpc consumer
 * server methods.
 *
 * <p>Users should use the typed constructors such as {@link
 * com.uber.data.kafka.consumerproxy.client.grpc.bytes.KafkaBytesServerMethodDefinition} instead of
 * this one.
 *
 * @see <a href="http://t.uber.com/kafka-grpc>t.uber.com/kafka-grpc</a>
 */
@Internal
final class KafkaServerMethodDefinition {
  private KafkaServerMethodDefinition() {}

  /**
   * Creates a new ServerMethodDefinition that should be included into a ServerServiceDefinition.
   *
   * @param consumerGroup that this method is associated with.
   * @param topic that this method is associated with.
   * @param unaryMethod is the handler that is executed when data for this method is received.
   * @param reqMarshaller is the marshaller for the request body.
   * @param <Req> is a generic request type.
   * @return ServerMethodDefinition that should be bound to a ServerServiceDefinition.
   * @implNote Per http://t.uber.com/kafka-grpc, Kafka consumer service is bound to a gRPC procedure
   *     named {@code consuemrGroup/topic}.
   */
  public static <Req> ServerMethodDefinition<Req, Empty> of(
      String consumerGroup,
      String topic,
      ServerCalls.UnaryMethod<Req, Empty> unaryMethod,
      MethodDescriptor.Marshaller<Req> reqMarshaller) {
    return ServerMethodDefinition.create(
        methodDescriptor(consumerGroup, topic, reqMarshaller),
        ServerCalls.asyncUnaryCall(unaryMethod));
  }

  private static <Req> MethodDescriptor<Req, Empty> methodDescriptor(
      String consumerGroup, String topic, MethodDescriptor.Marshaller<Req> reqMarshaller) {
    return MethodDescriptor.<Req, Empty>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName(generateGrpcMethodName(consumerGroup, topic))
        .setSampledToLocalTracing(true) // copy defaults from grpc proto generated method descriptor
        .setRequestMarshaller(reqMarshaller)
        .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
        .build();
  }

  private static String generateGrpcMethodName(String consumerGroup, String topic) {
    return MethodDescriptor.generateFullMethodName("kafka.consumerproxy." + consumerGroup, topic);
  }
}
