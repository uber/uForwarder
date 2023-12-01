package com.uber.data.kafka.consumerproxy.client.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCalls;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConsumerBytesServerMethodDefinition creates {@link io.grpc.ServerMethodDefinition} which should
 * be supplied to gRPC Server.
 *
 * <p>This library is a thin adapter that allows users to register Kafka Consumer Proxy gRPC
 * protocol (t.uber.com/kafka-grpc) compliant gRPC server methods for receiving data from Kafka
 * Consumer Proxy.
 *
 * <p>
 *
 * <ol>
 *   <li>Implement {@code io.grpc.stub.ServerCalls.UnaryMethod<ByteString,Empty>} to be executed
 *       when receiving messages from Kafka:
 *       <pre><code>
 *     // MyKafkaConusmerHandler is a handler for Kafka messages.
 *     public class MyKafkaConsumerHandler implements ServerCalls.UnaryMethod<ByteString, Empty> {
 *       {@literal @Override}
 *       public void invoke(ByteString data, StreamObserver<Empty> responseObserver) {
 *         // process data
 *
 *         // fetch Kafka specific metadata via the helper methods provided in ConsumerMetadata. e.g.,
 *         String topic = ConsumerMetadata.getTopic();
 *
 *         // Send back ack/retriableException/nonRetriableException by using helper methods
 *         // provided by ConsumerResponse.
 *         // You MUST call exactly one of ConsumerResponse.commit(), ConsumerResponse.retriableException(),
 *         // ConsumerResponse.nonRetriableException().
 *         ConsumerResponse.commit(responseObserver); // ack
 *         // ConsumerResponse.retriableException(responseObserver); // nack to retryQ
 *         // ConsumerResponse.nonRetriableException(responseObserver); // nack to DLQ
 *       }
 *     }
 *     </code></pre>
 *   <li><b>If using JFx</b>, supply {@link io.grpc.ServerMethodDefinition} for Kafka Consumer
 *       Handler as a {@code @Bean} in your Spring {@code @Configuration}.
 *       <p><b>NOTE: user support for proper usage of Spring dependency injection and JFx framework
 *       is supported by JFx team</b>
 *       <pre><code>
 *     // Your standard/existing Spring configuration class.
 *     // See JFx documentation or ask JFx team for more information about this.
 *     {@literal @Configuration}
 *     public class MyConfiguration {
 *       {@literal @Bean}
 *       public ServerMethodDefinition<ByteString, Empty> consumerGroupATopicOneConsumer() {
 *         // NOTE: you can and should load consumer group and topic from ConfigurationProperties
 *         // and provide the Handler as a separate bean. Follow JFx documentation for Spring DI
 *         // (or ask JFx team) if you need more details on how to do that.
 *         return KafkaBytesServerMethodDefinition.of(
 *           "consumerGroupA",
 *           "topicOne",
 *           new MyKafkaConsumerHandler()
 *         )
 *       }
 *     }
 *     </code></pre>
 *   <li><b>If NOT using JFx</b>, initialize a gRPC server with default middleware bundle:
 *       <p><b>NOTE: user support for proper usage of GrpcServer and Middleware bundle is supported
 *       by JFx team</b>
 *       <ul>
 *         <li>Add gRPC server and middleware libraries to BUCK file
 *             <pre>
 *           "//dev-platform/frameworks/jfx/library/grpc/server:src_main",
 *           "//dev-platform/frameworks/jfx/library/middleware/grpc/server:src_main",
 *           "//dev-platform/frameworks/jfx/library/middleware/grpc/server:src_main",
 *         </pre>
 *         <li>Create Grpc Server with default middleware bundle (NOTE: middleware bundle is
 *             REQUIRED to read Kafka metadata).
 *             <pre><code>
 *           // load service name (this should match your uDeploy service name
 *           String serviceName = "my-service-name";
 *
 *           // load gRPC port from $UBER_PORT_GRPC
 *           int grpcPort = Integer.parseInt(System.getenv("UBER_PORT_GRPC"));
 *
 *           // create gRPC server with defaults using JFx GrpcServerBuilder.
 *           GrpcServerBuilder grpcServerBuilder = GrpcServerBuilder.from(serviceName, grpcPort);
 *
 *           // add default set of logging, tracing, metric, etc interceptors to grpc server.
 *           JFxGrpcServerMiddlewareBundle.newBuilder(serviceName)
 *             .autoShutdown()
 *             .build()
 *             .apply(grpcServerBuilder);
 *
 *           // initialize a handler instance:
 *           MyKafkaConsumerHandler handler = new KafkaConsumerHandler();
 *
 *           // register the kafka handler to receive data for a group/topic pair with the gRPC server builder.
 *           grpcServerBuilder.addMethod(KafkaBytesServerMethodDefinition.of(group, topic, handler));
 *
 *           // build and start gRPC server
 *           grpcServerBuilder.build().start();
 *         </code></pre>
 *       </ul>
 * </ol>
 */
public final class ConsumerBytesServerMethodDefinition {
  private ConsumerBytesServerMethodDefinition() {}

  /**
   * Creates a new ServerMethodDefinition that should be included into a ServerServiceDefinition.
   *
   * @param consumerGroup that this method is associated with.
   * @param topic that this method is associated with.
   * @param unaryMethod is the handler that is executed when data for this method is received.
   * @return ServerMethodDefinition that should be bound to a ServerServiceDefinition.
   */
  public static ServerMethodDefinition<ByteString, Empty> of(
      String consumerGroup, String topic, ServerCalls.UnaryMethod<ByteString, Empty> unaryMethod) {
    return KafkaServerMethodDefinition.of(consumerGroup, topic, unaryMethod, new BytesMarshaller());
  }

  @VisibleForTesting
  static final class BytesMarshaller implements MethodDescriptor.Marshaller<ByteString> {
    private static final Logger logger = LoggerFactory.getLogger(BytesMarshaller.class);

    @Override
    public InputStream stream(ByteString value) {
      return value.newInput();
    }

    @Override
    public ByteString parse(InputStream stream) {
      try {
        return ByteString.readFrom(stream);
      } catch (IOException e) {
        logger.error("failed to parse InputStream into ByteString", e);
        throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withCause(e));
      }
    }
  }
}
