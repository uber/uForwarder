package com.uber.data.kafka.consumerproxy.container;

import static com.uber.data.kafka.consumerproxy.container.UForwarderContainer.CONTROLLER_GRPC_PORT;
import static com.uber.data.kafka.consumerproxy.testutils.Constants.JOB_GROUP_ID_DELIMITER;
import static com.uber.data.kafka.consumerproxy.testutils.Constants.MAX_AWAIT_TIME_IN_SEC;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.consumerproxy.client.grpc.ConsumerBytesServerMethodDefinition;
import com.uber.data.kafka.consumerproxy.testutils.Constants;
import com.uber.data.kafka.consumerproxy.testutils.KafkaUtils;
import com.uber.data.kafka.consumerproxy.testutils.MockConsumerServiceStarter;
import com.uber.data.kafka.consumerproxy.testutils.NetworkUtils;
import com.uber.data.kafka.consumerproxy.testutils.UForwarderUtils;
import com.uber.data.kafka.datatransfer.AddJobGroupRequest;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTaskGroup;
import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.Server;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.utility.DockerImageName;

public class UforwarderContainerIntegrationTests extends FievelTestBase {
  private static final Logger log =
      LoggerFactory.getLogger(UforwarderContainerIntegrationTests.class);
  // Docker images
  private static final DockerImageName TEST_IMAGE_ZOOKEEPER =
      DockerImageName.parse("bitnami/zookeeper:3.9.2");
  private static final DockerImageName TEST_IMAGE_KAFKA =
      DockerImageName.parse("confluentinc/cp-kafka:7.6.1");
  private static final DockerImageName TEST_IMAGE_UFORWARDER =
      DockerImageName.parse("uforwarder:0.1");
  // Simple consumer
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_TOPIC_NAME = "test-topic";
  private static String TEST_GROUP_NAME = "test-group";
  // Common settings
  private static String DOCKER_HOST_INTERNAL_ADDRESS = "host.docker.internal";
  private static int TEST_SERVICE_GRPC_PORT = 8085;
  private static int NUMBER_OF_MESSAGES = 5;
  private static Server mockConsumerServer;
  private static GenericContainer zkServer;
  private static KafkaContainer kafkaServer;
  private static UForwarderControllerContainer controller;
  private static UForwarderWorkerContainer worker;

  // works for both mac OS and linux
  @ClassRule public static final Network network = Network.newNetwork();

  @ClassRule
  public static final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @BeforeClass
  public static void setup() throws Exception {
    mockConsumerServer =
        MockConsumerServiceStarter.startTestService(
            TEST_GROUP_NAME,
            TEST_SERVICE_GRPC_PORT,
            ImmutableList.of(
                // Simple consumer
                ConsumerBytesServerMethodDefinition.of(
                    TEST_GROUP_NAME,
                    TEST_TOPIC_NAME,
                    new MockConsumerServiceStarter.TestKafkaConsumerHandler())));

    zkServer =
        new GenericContainer(TEST_IMAGE_ZOOKEEPER)
            .withExposedPorts(Constants.ZOOKEEPER_PORT)
            .withEnv("ALLOW_ANONYMOUS_LOGIN", "yes");
    zkServer.setNetwork(network);
    zkServer.start();
    NetworkUtils.assertPortInUseWithTimeout(zkServer.getMappedPort(Constants.ZOOKEEPER_PORT), 30);

    kafkaServer =
        new KafkaContainer(TEST_IMAGE_KAFKA)
            .withExternalZookeeper(
                NetworkUtils.getIpAddress(zkServer, network) + ":" + Constants.ZOOKEEPER_PORT);

    kafkaServer.setNetwork(network);
    kafkaServer.start();
    NetworkUtils.assertPortInUseWithTimeout(kafkaServer.getFirstMappedPort(), 30);

    UForwarderUtils.prepareZookeeperForController(
        String.format(
            "%s:%s", zkServer.getHost(), zkServer.getMappedPort(Constants.ZOOKEEPER_PORT)));
    String kafkaBootstrap =
        NetworkUtils.getIpAddress(kafkaServer, network) + ":" + Constants.KAFKA_PORT;
    controller =
        new UForwarderControllerContainer(TEST_IMAGE_UFORWARDER)
            .withImagePullPolicy(PullPolicy.defaultPolicy())
            .withExposedPorts(CONTROLLER_GRPC_PORT)
            .withZookeeperConnect(
                NetworkUtils.getIpAddress(zkServer, network) + ":" + Constants.ZOOKEEPER_PORT)
            .withKafkaBootstrapString(kafkaBootstrap);
    controller.setNetwork(network);
    controller.start();

    worker =
        new UForwarderWorkerContainer(TEST_IMAGE_UFORWARDER)
            .withImagePullPolicy(PullPolicy.defaultPolicy())
            .withController(
                NetworkUtils.getIpAddress(controller, network) + ":" + CONTROLLER_GRPC_PORT)
            .withKafkaBootstrapString(kafkaBootstrap);
    worker.setNetwork(network);
    worker.start();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    worker.stop();
    controller.stop();
    kafkaServer.stop();
    zkServer.stop();
    mockConsumerServer.shutdown();
  }

  @Test
  public void testKafkaConsumerProxyEnd2EndFlow() throws Exception {

    prepareTopic(TEST_TOPIC_NAME, kafkaServer.getBootstrapServers());
    String procedure =
        String.format(
            MockConsumerServiceStarter.METHOD_NAME_FORMAT, TEST_GROUP_NAME, TEST_TOPIC_NAME);

    UForwarderUtils.createJob(
        String.format(
            "%s:%d", controller.getHost(), controller.getMappedPort(CONTROLLER_GRPC_PORT)),
        getAddJobGroupRequest(TEST_TOPIC_NAME, TEST_GROUP_NAME, procedure, "", ImmutableList.of()));

    // Normal case
    sendKafkaMessages(
        TEST_TOPIC_NAME, kafkaServer.getBootstrapServers(), NUMBER_OF_MESSAGES, false);
    await()
        .atMost(240, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertTrue(
                  MockConsumerServiceStarter.TestKafkaConsumerHandler.INVOKE_COUNT.get()
                      >= NUMBER_OF_MESSAGES);
            });

    // Empty message case
    sendKafkaMessages(TEST_TOPIC_NAME, kafkaServer.getBootstrapServers(), NUMBER_OF_MESSAGES, true);
    await()
        .atMost(240, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertTrue(
                  MockConsumerServiceStarter.TestKafkaConsumerHandler.INVOKE_COUNT.get()
                      >= NUMBER_OF_MESSAGES);
            });
  }

  private AddJobGroupRequest getAddJobGroupRequest(
      String topicName,
      String consumerGroupName,
      String procedureName,
      String dlqTopicName,
      List<String> retryTopicNames) {
    RetryConfig.Builder retryConfigBuilder = RetryConfig.newBuilder();
    if (!retryTopicNames.isEmpty()) {
      retryConfigBuilder.setRetryEnabled(true);
      retryTopicNames.forEach(
          r ->
              retryConfigBuilder.addRetryQueues(
                  RetryQueue.newBuilder()
                      .setProcessingDelayMs(0)
                      .setMaxRetryCount(2)
                      .setRetryCluster(TEST_CLUSTER_NAME)
                      .setRetryQueueTopic(r)
                      .build()));
    }

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
                        .setUri(
                            "dns:///" + DOCKER_HOST_INTERNAL_ADDRESS + ":" + TEST_SERVICE_GRPC_PORT)
                        .setRpcTimeoutMs(1000)
                        .setProcedure(procedureName)
                        .setDlqCluster(TEST_CLUSTER_NAME)
                        .setDlqTopic(dlqTopicName)
                        .build())
                .setFlowControl(
                    FlowControl.newBuilder()
                        .setBytesPerSec(10)
                        .setMaxInflightMessages(10)
                        .setMessagesPerSec(10)
                        .build())
                .setRetryConfig(retryConfigBuilder)
                .build())
        .setJobGroupState(JobState.JOB_STATE_RUNNING)
        .build();
  }

  private void prepareTopic(String topicName, String bootstrapServers) {
    KafkaUtils.createTopic(topicName, 1, bootstrapServers);
  }

  private KafkaProducer<Byte[], Byte[]> prepareProducer(String bootstrapServers) {
    Properties producerProps = new Properties();
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "test");
    producerProps.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    return new KafkaProducer<>(producerProps);
  }

  private KafkaConsumer<Byte[], Byte[]> prepareConsumer(String bootstrapServers) {
    Properties consumerProps = new Properties();
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "test");
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "verify");

    consumerProps.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer<>(consumerProps);
  }

  private void sendKafkaMessages(
      String topicName, String bootstrapServers, int numberOfMessages, boolean isEmptyMessages)
      throws ExecutionException, InterruptedException {
    KafkaProducer producer = prepareProducer(bootstrapServers);
    KafkaConsumer consumer = prepareConsumer(bootstrapServers);

    try {
      // send sync messages
      for (int index = 0; index < numberOfMessages; index++) {
        ProducerRecord record =
            new ProducerRecord(topicName, String.format("test message %d", index).getBytes());
        if (isEmptyMessages) {
          record = new ProducerRecord(topicName, null);
        }
        producer.send(record).get();
      }
      consumer.subscribe(ImmutableList.of(topicName));
      await()
          .atMost(MAX_AWAIT_TIME_IN_SEC, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                ConsumerRecords records = consumer.poll(Duration.ofMillis(500));
                System.out.println("found num record:" + records != null ? records.count() : -1);
                Assert.assertTrue(records != null && records.count() != 0);
              });
    } finally {
      producer.close();
      consumer.close();
    }
  }
}
