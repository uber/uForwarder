package com.uber.data.kafka.uforwarder;

import com.uber.data.kafka.consumerproxy.client.grpc.ConsumerBytesServerMethodDefinition;
import com.uber.data.kafka.consumerproxy.client.grpc.ConsumerMetadata;
import com.uber.data.kafka.datatransfer.*;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class SampleConsumerAutoConfiguration {
  private static final Logger logger =
      LoggerFactory.getLogger(SampleConsumerAutoConfiguration.class);
  private static final String JOB_GROUP_ID_DELIMITER = "@";
  private static final String DOCKER_HOST_INTERNAL_ADDRESS = "host.docker.internal";
  private static final String TEST_CLUSTER_NAME = "dev";

  @Bean
  public StoredJobGroup createConsumer(
      ServerMethodDefinition methodDefinition,
      @Value("${group}") String group,
      @Value("${controllerConnect}") String controllerConnect,
      Server server,
      SampleConsumerProducer sampleConsumerProducer) {
    AddJobGroupRequest request =
        getAddJobGroupRequest(
            sampleConsumerProducer.topic(), group, methodDefinition, server.getPort());
    return Utils.createConsumerJob(controllerConnect, request);
  }

  private AddJobGroupRequest getAddJobGroupRequest(
      String topicName,
      String consumerGroupName,
      ServerMethodDefinition serverMethodDefinition,
      int port) {

    return AddJobGroupRequest.newBuilder()
        .setJobGroup(
            JobGroup.newBuilder()
                .setType(JobType.JOB_TYPE_KAFKA_CONSUMER_TO_RPC_DISPATCHER)
                .setJobGroupId(
                    String.join(
                        JOB_GROUP_ID_DELIMITER, topicName, TEST_CLUSTER_NAME, consumerGroupName))
                .setKafkaConsumerTaskGroup(
                    KafkaConsumerTaskGroup.newBuilder()
                        .setCluster(TEST_CLUSTER_NAME)
                        .setTopic(topicName)
                        .setConsumerGroup(consumerGroupName)
                        .build())
                .setRpcDispatcherTaskGroup(
                    RpcDispatcherTaskGroup.newBuilder()
                        .setUri("dns:///" + DOCKER_HOST_INTERNAL_ADDRESS + ":" + port)
                        .setRpcTimeoutMs(1000)
                        .setProcedure(
                            serverMethodDefinition.getMethodDescriptor().getFullMethodName())
                        .build())
                .setFlowControl(
                    FlowControl.newBuilder()
                        .setBytesPerSec(10)
                        .setMaxInflightMessages(10)
                        .setMessagesPerSec(10)
                        .build())
                .build())
        .setJobGroupState(JobState.JOB_STATE_RUNNING)
        .build();
  }

  @Bean
  public ServerMethodDefinition serverMethodDefinition(
      @Value("${group}") String group, @Value("${topic}") String topic) {
    return ConsumerBytesServerMethodDefinition.of(group, topic, new SampleConsumerHandler());
  }

  @Bean
  public Server GrpcServer(
      @Value("${grpc_port}") int grpcPort,
      @Value("${group}") String group,
      ServerMethodDefinition serverMethodDefinition)
      throws IOException {
    ServerServiceDefinition.Builder serverServiceDefinitionBuilder =
        ServerServiceDefinition.builder("kafka.consumerproxy." + group);

    // register the kafka handler to receive data for a group/topic pair with the gRPC server
    serverServiceDefinitionBuilder.addMethod(serverMethodDefinition);

    // start the server
    Server grpcServer =
        NettyServerBuilder.forPort(grpcPort)
            .addService(
                ServerInterceptors.intercept(
                    serverServiceDefinitionBuilder.build(), ConsumerMetadata.serverInterceptor()))
            .build();
    grpcServer.start();
    logger.info("Sample consumer service started on port " + grpcServer.getPort());
    return grpcServer;
  }
}
