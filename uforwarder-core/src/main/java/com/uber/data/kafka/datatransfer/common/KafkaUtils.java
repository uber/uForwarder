package com.uber.data.kafka.datatransfer.common;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;

public class KafkaUtils {

  // any start offset smaller than or equals to this value is invalid
  public static long MAX_INVALID_START_OFFSET = -1L;
  // any end offset smaller than or equals to this value is invalid
  public static long MAX_INVALID_END_OFFSET = 0L;
  // any offset to commit smaller than or equals to this value is invalid
  public static long MAX_INVALID_OFFSET_TO_COMMIT = 0L;

  public static final String SSL_ALGORITHM = "UPKI";
  public static final String SECURITY_PROTOCOL = "SSL";
  public static final String SECURITY_PROVIDER_CLASS =
      ("com.uber.kafka.security.provider.KafkaUPKIProviderCreator");

  public static Map<String, String> getSecurityConfigs() {
    ImmutableMap.Builder config = new ImmutableMap.Builder();
    config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SECURITY_PROTOCOL);
    config.put(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, SSL_ALGORITHM);
    config.put(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, SSL_ALGORITHM);
    config.put(SecurityConfig.SECURITY_PROVIDERS_CONFIG, SECURITY_PROVIDER_CLASS);
    return config.build();
  }

  /**
   * Creates {@link KafkaClusterResolver} instance by full class name
   *
   * @param className the full class name
   * @return the kafka cluster resolver
   */
  public static KafkaClusterResolver newResolverByClassName(String className) {
    try {
      Class cls = Class.forName(className);
      return (KafkaClusterResolver) cls.getConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }
}
