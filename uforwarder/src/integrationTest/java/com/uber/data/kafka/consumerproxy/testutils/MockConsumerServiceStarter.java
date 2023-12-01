package com.uber.data.kafka.consumerproxy.testutils;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.uber.data.kafka.consumerproxy.client.grpc.ConsumerMetadata;
import com.uber.data.kafka.consumerproxy.client.grpc.ConsumerResponse;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities to start a KCP consumer service */
public class MockConsumerServiceStarter {

  private static final Logger logger = LoggerFactory.getLogger(MockConsumerServiceStarter.class);
  private static final String PHYSICAL_TOPIC_PATTERN = "physical-topic=(.+),physical-partition";
  private static final int MAX_AWAIT_TIME_IN_SEC = 30;
  public static String METHOD_NAME_FORMAT = "kafka.consumerproxy.%s/%s";

  private MockConsumerServiceStarter() {}

  /**
   * Starts test consumer service
   *
   * @param groupName the consumer group name
   * @param grpcPort the grpc port
   * @param methodDefinitionList the list of to-be-registered methods
   * @throws IOException
   */
  public static Server startTestService(
      String groupName, int grpcPort, List<ServerMethodDefinition> methodDefinitionList)
      throws IOException {
    ServerServiceDefinition.Builder serverServiceDefinitionBuilder =
        ServerServiceDefinition.builder("kafka.consumerproxy." + groupName);

    // register the kafka handler to receive data for a group/topic pair with the gRPC server
    // builder.
    methodDefinitionList.forEach(serverServiceDefinitionBuilder::addMethod);

    // start the server
    Server grpcServer =
        NettyServerBuilder.forPort(grpcPort)
            .addService(
                ServerInterceptors.intercept(
                    serverServiceDefinitionBuilder.build(), ConsumerMetadata.serverInterceptor()))
            .build();
    // this will leak a thread.
    // this is okay b/c start/stop is only invoked on unittest.
    grpcServer.start();
    NetworkUtils.assertPortInUseWithTimeout(grpcPort, MAX_AWAIT_TIME_IN_SEC);
    logger.info("mock consumer service started");
    return grpcServer;
  }

  // TestKafkaConsumerHandler is a handler for ACKING Kafka messages only.
  public static class TestKafkaConsumerHandler
      implements ServerCalls.UnaryMethod<ByteString, Empty> {
    public static AtomicInteger INVOKE_COUNT = new AtomicInteger(0);

    @Override
    public void invoke(ByteString data, StreamObserver responseObserver) {
      // fetch Kafka specific metadata via the helper methods provided in KafkaMetadata. e.g.,
      String topic = ConsumerMetadata.getTopic();
      INVOKE_COUNT.incrementAndGet();
      ConsumerResponse.commit(responseObserver); // ack
    }
  }

  // NackingTestKafkaConsumerHandler is a handler for NACKING Kafka messages.
  public static class NackingTestKafkaConsumerHandler
      implements ServerCalls.UnaryMethod<ByteString, Empty> {
    public static AtomicInteger INVOKE_COUNT = new AtomicInteger(0);

    @Override
    public void invoke(ByteString data, StreamObserver responseObserver) {
      INVOKE_COUNT.incrementAndGet();
      // nack messages into RQ first
      long retryCount = ConsumerMetadata.getRetryCount();
      logger.info("received a message data={}, retryCount={}", data, retryCount);
      if (retryCount == 0) {
        ConsumerResponse.retriableException(responseObserver);
      } else {
        // the second time, nack into DLQ
        ConsumerResponse.nonRetriableException(responseObserver);
      }
    }
  }

  // RetryingTestKafkaConsumerHandler is a handler for NACKING Kafka messages.
  // The handler will always return retriable exception for all Kafka messages.
  public static class RetryingTestKafkaConsumerHandler
      implements ServerCalls.UnaryMethod<ByteString, Empty> {
    public static AtomicInteger INVOKE_COUNT = new AtomicInteger(0);
    public static Map<Long, AtomicInteger> RETRY_COUNT_TO_INVOKE_COUNT_MAP =
        new ConcurrentHashMap<>();
    public static Map<String, AtomicInteger> PHYSICAL_SOURCE_TO_INVOKE_COUNT_MAP =
        new ConcurrentHashMap<>();

    @Override
    public void invoke(ByteString data, StreamObserver responseObserver) {
      INVOKE_COUNT.incrementAndGet();
      // Record retry count info
      long retryCount = ConsumerMetadata.getRetryCount();
      RETRY_COUNT_TO_INVOKE_COUNT_MAP.putIfAbsent(retryCount, new AtomicInteger(0));
      RETRY_COUNT_TO_INVOKE_COUNT_MAP.get(retryCount).incrementAndGet();
      // Record incoming topic info
      String tracingInfo = ConsumerMetadata.getTracingInfo();
      String physicalTopic = "ORIGINAL_TOPIC";
      Pattern pattern = Pattern.compile(PHYSICAL_TOPIC_PATTERN);
      Matcher m = pattern.matcher(tracingInfo);
      if (m.find()) {
        physicalTopic = m.group(1);
        PHYSICAL_SOURCE_TO_INVOKE_COUNT_MAP.putIfAbsent(physicalTopic, new AtomicInteger(0));
        PHYSICAL_SOURCE_TO_INVOKE_COUNT_MAP.get(physicalTopic).incrementAndGet();
      }
      logger.info(
          "received a message data={}, retryCount={}, tracingInfo={}, physicalTopic={}",
          data,
          retryCount,
          tracingInfo,
          physicalTopic);

      ConsumerResponse.retriableException(responseObserver);
    }
  }
}
