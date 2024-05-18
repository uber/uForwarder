package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.utils.NodeConfigurationUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("uforwarder-worker")
public class NodeAutoConfiguration {
  @Bean(name = "serverPort")
  public int serverPort(@Value("${server.port}") int port) {
    return port;
  }

  /**
   * Returns the {@code Node} information for this JVM.
   *
   * @return Node to be used within uforwarder framework.
   */
  @Bean
  public Node node(@Qualifier("serverPort") int port) {
    return Node.newBuilder().setHost(NodeConfigurationUtils.getHost()).setPort(port).build();
  }
}
