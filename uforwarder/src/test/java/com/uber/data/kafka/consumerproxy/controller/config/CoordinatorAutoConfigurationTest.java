package com.uber.data.kafka.consumerproxy.controller.config;

import com.uber.data.kafka.consumerproxy.controller.confg.CoordinatorAutoConfiguration;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.controller.config.ZookeeperConfiguration;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class CoordinatorAutoConfigurationTest {
  private Node node;
  private Scope scope;
  private Tracer tracer;
  private TestingServer zkServer;

  ZookeeperConfiguration config;
  CoordinatorAutoConfiguration autoConfiguration;

  @BeforeEach
  public void setup() throws Exception {
    zkServer = new TestingServer(-1, true);
    int port = zkServer.getPort();

    node = Node.newBuilder().setHost("localhost").setPort(8000).build();
    scope = new NoopScope();
    tracer = new MockTracer();
    config = new ZookeeperConfiguration();
    config.setZkConnection("localhost:" + port + "/uforwarder");
    config.setAutoCreateRootNode(true);
    autoConfiguration = new CoordinatorAutoConfiguration();
  }

  @AfterEach
  public void teardown() throws Exception {
    zkServer.close();
  }

  @Test
  @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
  public void testLeaderSelector() throws Exception {
    Assertions.assertNotNull(
        autoConfiguration.leaderSelector(
            config, node, CoreInfra.builder().withScope(scope).withTracer(tracer).build()));
  }
}
