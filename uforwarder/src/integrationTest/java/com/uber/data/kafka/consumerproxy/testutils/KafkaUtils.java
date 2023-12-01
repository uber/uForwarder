package com.uber.data.kafka.consumerproxy.testutils;

import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities to start Kafka during unit tests. Copied from
 * https://github.com/uber/uReplicator/blob/master/uReplicator-Common/src/test/java/com/uber/stream/kafka/mirrormaker/common/utils/KafkaStarterUtils.java
 * and
 * data/kafka/ecosystem-manager/kafka-admin-service-v2/src/test/java/com/uber/data/kafka/admin/service/utils/KafkaStarterUtils.java
 */
public class KafkaUtils {

  private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);
  private static final int MAX_AWAIT_TIME_IN_SEC = 30;

  /**
   * Creates kafka topic
   *
   * @param topicName the topic name
   * @param bootstrapServer kafka bootstrap server list
   */
  public static void createTopic(String topicName, String bootstrapServer) {
    createTopic(topicName, 1, bootstrapServer);
  }

  /**
   * Creates kafka topic
   *
   * @param topicName the topic name
   * @param numOfPartitions number of partitions to create
   * @param bootstrapServers kafka bootstrap server list
   */
  public static void createTopic(String topicName, int numOfPartitions, String bootstrapServers) {
    try {
      getAdminClient(
          bootstrapServers,
          (client -> {
            NewTopic newTopic = new NewTopic(topicName, numOfPartitions, (short) 1);
            client.createTopics(ImmutableList.of(newTopic));
            return true;
          }));

    } catch (TopicExistsException e) {
      // Catch TopicExistsException otherwise it will break maven-surefire-plugin
      logger.warn("Topic {} already existed", topicName);
    }

    // validates topic is created
    await()
        .atMost(MAX_AWAIT_TIME_IN_SEC, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Assert.assertTrue(checkTopicExistence(topicName, bootstrapServers));
            });
  }

  /**
   * Checks topic exist or not
   *
   * @param topicName kafka topic name
   * @param bootstrapServers kafka bootstrap server list
   * @return whether topic exist or not
   */
  public static boolean checkTopicExistence(String topicName, String bootstrapServers) {
    return getAdminClient(
        bootstrapServers,
        (client -> {
          Map<String, KafkaFuture<TopicDescription>> topics =
              client.describeTopics(ImmutableList.of(topicName)).values();

          try {
            return topics.containsKey(topicName)
                && topics.get(topicName).get().name().equals(topicName);
          } catch (InterruptedException e) {
            logger.error("error on checkTopicExistence", e);
            return false;
          } catch (ExecutionException e) {
            logger.error("error on checkTopicExistence", e);
            return false;
          }
        }));
  }

  private static <Rep> Rep getAdminClient(
      String bootstrapServer, Function<AdminClient, Rep> function) {
    AdminClient adminClient =
        KafkaAdminClient.create(
            ImmutableMap.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServer,
                AdminClientConfig.CLIENT_ID_CONFIG,
                "kcp-unittest"));
    try {
      return function.apply(adminClient);
    } finally {
      adminClient.close();
    }
  }
}
