package com.uber.data.kafka.consumerproxy;

import static com.uber.data.kafka.consumerproxy.testutils.Constants.JOB_GROUP_ID_DELIMITER;
import static com.uber.data.kafka.consumerproxy.testutils.Constants.MAX_AWAIT_TIME_IN_SEC;
import static com.uber.data.kafka.consumerproxy.testutils.Constants.ZOOKEEPER_PORT;
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.consumerproxy.client.grpc.ConsumerBytesServerMethodDefinition;
import com.uber.data.kafka.consumerproxy.testutils.Constants;
import com.uber.data.kafka.consumerproxy.testutils.KafkaUtils;
import com.uber.data.kafka.consumerproxy.testutils.MockConsumerServiceStarter;
import com.uber.data.kafka.consumerproxy.testutils.NetworkUtils;
import com.uber.data.kafka.consumerproxy.testutils.UForwarderStarter;
import com.uber.data.kafka.consumerproxy.testutils.UForwarderUtils;
import com.uber.data.kafka.datatransfer.AddJobGroupRequest;
import com.uber.data.kafka.datatransfer.AddJobGroupResponse;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.GetAllJobGroupsRequest;
import com.uber.data.kafka.datatransfer.GetAllJobGroupsResponse;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobType;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskGroup;
import com.uber.data.kafka.datatransfer.MasterAdminServiceGrpc;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTaskGroup;
import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.stub.MetadataUtils;
import java.time.Duration;
import java.util.Iterator;
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
import org.testcontainers.utility.DockerImageName;

public class UforwarderIntegrationTests extends FievelTestBase {
  private static final Logger log = LoggerFactory.getLogger(UforwarderIntegrationTests.class);
  // Docker images
  private static final DockerImageName TEST_IMAGE_ZOOKEEPER =
      DockerImageName.parse("bitnami/zookeeper:3.9.2");
  private static final DockerImageName TEST_IMAGE_KAFKA =
      DockerImageName.parse("confluentinc/cp-kafka:7.6.1");
  // Simple consumer
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_TOPIC_NAME = "test-topic";
  // RQ + DLQ
  private static String TEST_TOPIC_2_NAME = "test-topic-2";
  private static String TEST_RETRY_TOPIC_NAME = "test-topic-2__test-group__retry";
  private static String TEST_TOPIC_2_DLQ_TOPIC_NAME = "test-topic-2__test-group__dlq";
  // TRQ
  private static String TEST_TOPIC_3_NAME = "test-topic-3";
  private static String TEST_TIER_1_RETRY_TOPIC_NAME = "test-topic-3__test-group__1__retry";
  private static String TEST_TIER_2_RETRY_TOPIC_NAME = "test-topic-3__test-group__2__retry";
  private static String TEST_TOPIC_3_DLQ_TOPIC_NAME = "test-topic-3__test-group__dlq";
  // Common settings
  private static String TEST_GROUP_NAME = "test-group";
  private static final String ZK_CONNECT_TEMPLATE = "localhost:%s";
  private static int NUMBER_OF_MESSAGES = 5;
  private static Server mockConsumerServer;
  private static GenericContainer zkServer;
  private static KafkaContainer kafkaServer;
  private static int controllerGrpcPort;

  // works for both mac OS and linux
  @ClassRule public static final Network network = Network.newNetwork();

  @ClassRule
  public static final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @BeforeClass
  public static void setup() throws Exception {
    mockConsumerServer =
        MockConsumerServiceStarter.startTestService(
            TEST_GROUP_NAME,
            ImmutableList.of(
                // Simple consumer
                ConsumerBytesServerMethodDefinition.of(
                    TEST_GROUP_NAME,
                    TEST_TOPIC_NAME,
                    new MockConsumerServiceStarter.TestKafkaConsumerHandler()),
                // RQ + DLQ
                ConsumerBytesServerMethodDefinition.of(
                    TEST_GROUP_NAME,
                    TEST_TOPIC_2_NAME,
                    new MockConsumerServiceStarter.NackingTestKafkaConsumerHandler()),
                // TRQ
                ConsumerBytesServerMethodDefinition.of(
                    TEST_GROUP_NAME,
                    TEST_TOPIC_3_NAME,
                    new MockConsumerServiceStarter.RetryingTestKafkaConsumerHandler())));
    zkServer =
        new GenericContainer(TEST_IMAGE_ZOOKEEPER)
            .withExposedPorts(Constants.ZOOKEEPER_PORT)
            .withEnv("ALLOW_ANONYMOUS_LOGIN", "yes");
    zkServer.setNetwork(network);
    zkServer.start();
    NetworkUtils.assertPortInUseWithTimeout(zkServer.getMappedPort(ZOOKEEPER_PORT), 30);

    kafkaServer =
        new KafkaContainer(TEST_IMAGE_KAFKA)
            .withExternalZookeeper(
                NetworkUtils.getIpAddress(zkServer, network) + ":" + ZOOKEEPER_PORT);
    kafkaServer.setNetwork(network);
    kafkaServer.start();
    NetworkUtils.assertPortInUseWithTimeout(kafkaServer.getFirstMappedPort(), 30);

    controllerGrpcPort = NetworkUtils.getRandomAvailablePort();
    UForwarderStarter.startUForwarderMaster(
        kafkaServer.getBootstrapServers(),
        String.format(ZK_CONNECT_TEMPLATE, zkServer.getMappedPort(ZOOKEEPER_PORT)),
        controllerGrpcPort);
    UForwarderStarter.startUForwarderWorker(
        kafkaServer.getBootstrapServers(),
        NetworkUtils.getRandomAvailablePort(),
        Constants.MASTER_HOST + ":" + controllerGrpcPort);
  }

  @AfterClass
  public static void shutdown() throws Exception {
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
        String.format("%s:%s", Constants.MASTER_HOST, controllerGrpcPort),
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

  @Test
  public void testKafkaConsumerProxyEnd2EndFlowWithRQAndDLQ() throws Exception {
    String procedure =
        String.format(
            MockConsumerServiceStarter.METHOD_NAME_FORMAT, TEST_GROUP_NAME, TEST_TOPIC_2_NAME);
    prepareTopic(TEST_TOPIC_2_NAME, kafkaServer.getBootstrapServers());
    prepareTopic(TEST_RETRY_TOPIC_NAME, kafkaServer.getBootstrapServers());
    prepareTopic(TEST_TOPIC_2_DLQ_TOPIC_NAME, kafkaServer.getBootstrapServers());
    sendKafkaMessages(
        TEST_TOPIC_2_NAME, kafkaServer.getBootstrapServers(), NUMBER_OF_MESSAGES, false);
    UForwarderUtils.createJob(
        String.format("%s:%s", Constants.MASTER_HOST, controllerGrpcPort),
        getAddJobGroupRequest(
            TEST_TOPIC_2_NAME,
            TEST_GROUP_NAME,
            procedure,
            TEST_TOPIC_2_DLQ_TOPIC_NAME,
            ImmutableList.of(TEST_RETRY_TOPIC_NAME)));
    UForwarderUtils.createJob(
        String.format("%s:%s", Constants.MASTER_HOST, controllerGrpcPort),
        getAddJobGroupRequest(
            TEST_RETRY_TOPIC_NAME,
            TEST_GROUP_NAME,
            procedure,
            TEST_TOPIC_2_DLQ_TOPIC_NAME,
            ImmutableList.of(TEST_RETRY_TOPIC_NAME)));
    UForwarderUtils.createJob(
        String.format("%s:%s", Constants.MASTER_HOST, controllerGrpcPort),
        getAddJobGroupRequest(
            TEST_TOPIC_2_DLQ_TOPIC_NAME,
            TEST_GROUP_NAME,
            procedure,
            TEST_TOPIC_2_DLQ_TOPIC_NAME,
            ImmutableList.of(TEST_RETRY_TOPIC_NAME)));

    await()
        .atMost(240, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertTrue(
                  MockConsumerServiceStarter.NackingTestKafkaConsumerHandler.INVOKE_COUNT.get()
                      // The test handler should receive 3 copies of messages from
                      // original queue, retry queue, and DLQ
                      >= NUMBER_OF_MESSAGES * 3);
            });
  }

  @Test
  public void testKafkaConsumerProxyEnd2EndFlowWithTieredRQ() throws Exception {
    String procedure =
        String.format(
            MockConsumerServiceStarter.METHOD_NAME_FORMAT, TEST_GROUP_NAME, TEST_TOPIC_3_NAME);
    prepareTopic(TEST_TOPIC_3_NAME, kafkaServer.getBootstrapServers());
    prepareTopic(TEST_TIER_1_RETRY_TOPIC_NAME, kafkaServer.getBootstrapServers());
    prepareTopic(TEST_TIER_2_RETRY_TOPIC_NAME, kafkaServer.getBootstrapServers());
    prepareTopic(TEST_TOPIC_3_DLQ_TOPIC_NAME, kafkaServer.getBootstrapServers());
    sendKafkaMessages(
        TEST_TOPIC_3_NAME, kafkaServer.getBootstrapServers(), NUMBER_OF_MESSAGES, false);
    UForwarderUtils.createJob(
        String.format("%s:%s", Constants.MASTER_HOST, controllerGrpcPort),
        getAddJobGroupRequest(
            TEST_TOPIC_3_NAME,
            TEST_GROUP_NAME,
            procedure,
            TEST_TOPIC_3_DLQ_TOPIC_NAME,
            ImmutableList.of(TEST_TIER_1_RETRY_TOPIC_NAME, TEST_TIER_2_RETRY_TOPIC_NAME)));
    UForwarderUtils.createJob(
        String.format("%s:%s", Constants.MASTER_HOST, controllerGrpcPort),
        getAddJobGroupRequest(
            TEST_TIER_1_RETRY_TOPIC_NAME,
            TEST_GROUP_NAME,
            procedure,
            TEST_TOPIC_3_DLQ_TOPIC_NAME,
            ImmutableList.of(TEST_TIER_1_RETRY_TOPIC_NAME, TEST_TIER_2_RETRY_TOPIC_NAME)));
    UForwarderUtils.createJob(
        String.format("%s:%s", Constants.MASTER_HOST, controllerGrpcPort),
        getAddJobGroupRequest(
            TEST_TIER_2_RETRY_TOPIC_NAME,
            TEST_GROUP_NAME,
            procedure,
            TEST_TOPIC_3_DLQ_TOPIC_NAME,
            ImmutableList.of(TEST_TIER_1_RETRY_TOPIC_NAME, TEST_TIER_2_RETRY_TOPIC_NAME)));
    UForwarderUtils.createJob(
        String.format("%s:%s", Constants.MASTER_HOST, controllerGrpcPort),
        getAddJobGroupRequest(
            TEST_TOPIC_3_DLQ_TOPIC_NAME,
            TEST_GROUP_NAME,
            procedure,
            TEST_TOPIC_3_DLQ_TOPIC_NAME,
            ImmutableList.of(TEST_TIER_1_RETRY_TOPIC_NAME, TEST_TIER_2_RETRY_TOPIC_NAME)));

    await()
        .atMost(240, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              // The test handler should receive 6 copies of messages from
              // original queue, T1 retry queue(x2), T2 retry queue(x2), and DLQ
              Assert.assertTrue(
                  MockConsumerServiceStarter.RetryingTestKafkaConsumerHandler.INVOKE_COUNT.get()
                      >= NUMBER_OF_MESSAGES * 6);
              for (long i = 0L; i < 6; i++) {
                Assert.assertTrue(
                    MockConsumerServiceStarter.RetryingTestKafkaConsumerHandler
                            .RETRY_COUNT_TO_INVOKE_COUNT_MAP
                            .get(i)
                            .get()
                        >= NUMBER_OF_MESSAGES);
              }
              // T1 retry queue
              Assert.assertTrue(
                  MockConsumerServiceStarter.RetryingTestKafkaConsumerHandler
                          .PHYSICAL_SOURCE_TO_INVOKE_COUNT_MAP
                          .get(TEST_TIER_1_RETRY_TOPIC_NAME)
                          .get()
                      >= NUMBER_OF_MESSAGES * 2);
              // T2 retry queue
              Assert.assertTrue(
                  MockConsumerServiceStarter.RetryingTestKafkaConsumerHandler
                          .PHYSICAL_SOURCE_TO_INVOKE_COUNT_MAP
                          .get(TEST_TIER_2_RETRY_TOPIC_NAME)
                          .get()
                      >= NUMBER_OF_MESSAGES * 2);
              // DLQ
              Assert.assertTrue(
                  MockConsumerServiceStarter.RetryingTestKafkaConsumerHandler
                          .PHYSICAL_SOURCE_TO_INVOKE_COUNT_MAP
                          .get(TEST_TOPIC_3_DLQ_TOPIC_NAME)
                          .get()
                      >= NUMBER_OF_MESSAGES);
            });
  }

  private void addJob(AddJobGroupRequest addJobGroupRequest) {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(Constants.MASTER_HOST, controllerGrpcPort)
            .usePlaintext()
            .build();
    MasterAdminServiceGrpc.MasterAdminServiceBlockingStub blockingStub =
        MetadataUtils.attachHeaders(
            MasterAdminServiceGrpc.newBlockingStub(channel), new Metadata());
    AddJobGroupResponse response = blockingStub.addJobGroup(addJobGroupRequest);
    Assert.assertTrue(response.hasGroup());
    Assert.assertNotNull(response.getGroup().getJobGroup());

    await()
        .atMost(Constants.MAX_AWAIT_TIME_IN_SEC, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Iterator<GetAllJobGroupsResponse> allJobGroups =
                  blockingStub.getAllJobGroups(GetAllJobGroupsRequest.newBuilder().build());
              Assert.assertTrue(allJobGroups.hasNext());
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
                        .setUri("dns:///localhost:" + mockConsumerServer.getPort())
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
