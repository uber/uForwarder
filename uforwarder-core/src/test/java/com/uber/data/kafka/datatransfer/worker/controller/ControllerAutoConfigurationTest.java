package com.uber.data.kafka.datatransfer.worker.controller;

import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.CoreInfraAutoConfiguration;
import com.uber.data.kafka.datatransfer.common.HostResolver;
import com.uber.data.kafka.datatransfer.common.StaticResolver;
import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineManager;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Scope;
import io.opentracing.Tracer;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(
  classes = {
    CoreInfraAutoConfiguration.class,
    ControllerAutoConfiguration.class,
    ControllerAutoConfigurationTest.MockNodeAutoConfiguration.class
  }
)
@ActiveProfiles(profiles = {"data-transfer-worker"})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class ControllerAutoConfigurationTest extends FievelTestBase {

  @Configuration
  static class MockNodeAutoConfiguration {
    @Bean
    public Node getNode() {
      return Node.newBuilder().setHost("localhost").setPort(1111).build();
    }
  }

  @MockBean Scope scope;

  @MockBean Tracer tracer;
  @MockBean PipelineManager pipelineManager;

  @Autowired ControllerAutoConfiguration autoConfiguration;
  @Autowired GrpcControllerConfiguration config;
  @Autowired GrpcController grpcController;
  @Autowired HostResolver masterResolver;

  @Test
  public void test() {
    Assert.assertNotNull(config);
    Assert.assertEquals("localhost:9000", config.getMasterHostPort());
    Assert.assertEquals("udg://master", config.getMasterUdgPath());
    Assert.assertEquals(Duration.ofMinutes(1), config.getHeartbeatInterval());
    Assert.assertEquals(Duration.ofMinutes(2), config.getHeartbeatTimeout());
    Assert.assertEquals(Duration.ofMinutes(3), config.getWorkerLease());
    Assert.assertEquals(4, config.getCommandExecutorPoolSize());
    Assert.assertNotNull(grpcController);
    Assert.assertNotNull(masterResolver);
  }

  @Test
  public void testStaticController() {
    // make separate config b/c modifiying the autowired configuration seems to impact other tests.
    GrpcControllerConfiguration staticControllerConfig = new GrpcControllerConfiguration();
    Assert.assertNotNull(autoConfiguration.masterClientResolver(staticControllerConfig));
    Assert.assertTrue(
        autoConfiguration.masterClientResolver(staticControllerConfig) instanceof StaticResolver);
  }
}
