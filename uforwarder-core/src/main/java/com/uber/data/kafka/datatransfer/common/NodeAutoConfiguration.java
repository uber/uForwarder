package com.uber.data.kafka.datatransfer.common;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.management.NodeUrlResolver;
import com.uber.data.kafka.datatransfer.utils.NodeConfigurationUtils;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

@Configuration
@Profile({"uforwarder-worker", "uforwarder-controller"})
public class NodeAutoConfiguration {
  /**
   * Returns the {@code Node} information for this JVM.
   *
   * @return Node to be used within uforwarder framework.
   */
  @Bean
  public Node node(Environment environment) {
    return Node.newBuilder()
        .setHost(NodeConfigurationUtils.getHost())
        .setPort(getPort(environment))
        .build();
  }

  /**
   * Returns the {@NodeUrlResolver} to resolve the url of the node for debug page
   *
   * @return the node url resolver
   */
  @Bean
  public NodeUrlResolver nodeUrlResolver() {
    return new NodeUrlResolver();
  }

  /**
   * Gets node port for controller, the port should be GRPC port for GRPC API forwarding for worker,
   * the port should be http port for Debug UI
   *
   * @param environment
   * @return node port
   */
  private int getPort(Environment environment) {
    List<String> activeProfiles = ImmutableList.copyOf(environment.getActiveProfiles());
    String portStr;
    if (activeProfiles.contains("uforwarder-worker")) {
      portStr = environment.getProperty("server.port");
    } else {
      portStr = environment.getProperty("grpc.port");
    }

    return Integer.parseInt(portStr);
  }
}
