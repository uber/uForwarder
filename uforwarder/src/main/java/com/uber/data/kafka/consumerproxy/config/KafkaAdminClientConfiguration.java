package com.uber.data.kafka.consumerproxy.config;

import com.uber.data.kafka.datatransfer.common.KafkaClusterResolver;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configurations that are used to instantiate a {@link
 * com.uber.data.kafka.clients.admin.MultiClusterAdmin} client.
 */
@ConfigurationProperties(prefix = "master.kafka.admin")
public class KafkaAdminClientConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(KafkaAdminClientConfiguration.class);
  private static final Scope noopScope = new NoopScope();

  // add a suffix to client ID so each Kafka AdminClient has a unique client ID
  private final AtomicInteger CLIENT_ID_SUFFIX = new AtomicInteger(0);
  private final ConcurrentMap<String, Integer> clusterClientIdMap = new ConcurrentHashMap<>();

  // The follow configurations are used outside of the KafkaAdminClient
  private String resolverClass = KafkaClusterResolver.class.getName();

  // The following configurations are passed through to KafkaAdminClient constructor.
  private String clientId = "kafka-consumer-proxy-admin";
  private String bootstrapServers = "localhost:9092";
  private boolean enableSecure = true;

  // These are required configuration for KafkaConsumer class but we do not need to expose them
  // b/c this consumer is only used for offsetForTimes query.
  private final String deserializerClass =
      "org.apache.kafka.common.serialization.ByteArrayDeserializer";

  public String getClientId(String cluster) {
    clusterClientIdMap.computeIfAbsent(cluster, c -> CLIENT_ID_SUFFIX.getAndIncrement());
    return clientId + clusterClientIdMap.getOrDefault(cluster, 0);
  }

  public String getClientId() {
    return clientId + CLIENT_ID_SUFFIX.getAndIncrement();
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public String getBootstrapServer(String clusterName) throws Exception {
    // Make connection secure. As KCP is super user it will be able to connect to all topics.
    return KafkaUtils.newResolverByClassName(resolverClass)
        .getBootstrapServers(clusterName, bootstrapServers, true);
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getResolverClass() {
    return resolverClass;
  }

  public void setResolverClass(String resolverClass) {
    this.resolverClass = resolverClass;
  }

  public boolean isEnableSecure() {
    return enableSecure;
  }

  public void setEnableSecure(boolean enableSecure) {
    this.enableSecure = enableSecure;
  }

  public Properties getProperties(String cluster) {
    return Instrumentation.instrument.withRuntimeException(
        logger,
        noopScope, // noop scope b/c we only want error logging.
        () -> {
          Properties properties = new Properties();
          properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, getClientId(cluster));
          properties.setProperty(
              CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServer(cluster));
          properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClass);
          properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass);
          if (enableSecure) {
            for (Map.Entry<String, String> entry : KafkaUtils.getSecurityConfigs().entrySet()) {
              properties.setProperty(entry.getKey(), entry.getValue());
            }
          }
          return properties;
        },
        "kafka-admin-client-configuration.get-properties");
  }
}
