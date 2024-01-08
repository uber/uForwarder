package com.uber.data.kafka.consumerproxy.controller.config;

import com.uber.data.kafka.consumerproxy.controller.confg.CoordinatorAutoConfiguration;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.controller.config.ZookeeperConfiguration;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import io.opentracing.Tracer;
import io.opentracing.mock.MockTracer;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CoordinatorAutoConfigurationTest extends FievelTestBase {
  private Node node;
  private Scope scope;
  private Tracer tracer;
  private TestingServer zkServer;

  ZookeeperConfiguration config;
  CoordinatorAutoConfiguration autoConfiguration;

  @Before
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

  @After
  public void teardown() throws Exception {
    zkServer.close();
  }

  @Test(timeout = 10000)
  public void testLeaderSelector() throws Exception {
    Assert.assertNotNull(
        autoConfiguration.leaderSelector(
            config, node, CoreInfra.builder().withScope(scope).withTracer(tracer).build()));
  }
}
