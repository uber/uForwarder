package com.uber.data.kafka.consumerproxy.worker;

import com.uber.data.kafka.consumerproxy.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcDispatcherFactory;
import com.uber.data.kafka.consumerproxy.worker.filter.Filter;
import com.uber.data.kafka.consumerproxy.worker.filter.OriginalClusterFilter;
import com.uber.data.kafka.consumerproxy.worker.limiter.AdaptiveInflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.limiter.LongFixedInflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.processor.ProcessorFactory;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineFactory;
import com.uber.data.kafka.datatransfer.worker.pipelines.PipelineMetricPublisher;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {UForwarderWorkerFactory.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
@ActiveProfiles({"data-transfer-worker", "uforwarder-worker"})
public class UForwarderWorkerFactoryTest extends FievelTestBase {

  @Autowired ProcessorFactory processorFactory;

  @Autowired LongFixedInflightLimiter longFixedInflightLimiter;

  @Autowired DynamicConfiguration dynamicConfiguration;

  @Autowired GrpcDispatcherFactory grpcDispatcherFactory;

  @Autowired PipelineFactory pipelineFactory;

  @Autowired PipelineMetricPublisher pipelineMetricPublisher;

  @Autowired AdaptiveInflightLimiter.Builder adaptiveInflightLimiterBuilder;

  @Autowired Node node;

  @Autowired Filter.Factory filterFactory;

  @Test
  public void testProcessorFactory() {
    Assert.assertNotNull(this.processorFactory);
  }

  @Test
  public void testLongFixedInflightLimiter() {
    Assert.assertNotNull(this.longFixedInflightLimiter);
  }

  @Test
  public void testDynamicConfiguration() {
    Assert.assertNotNull(dynamicConfiguration);
  }

  @Test
  public void testGrpcDispatcherFactory() {
    Assert.assertNotNull(grpcDispatcherFactory);
  }

  @Test
  public void testPipelineMetricPublisher() {
    Assert.assertNotNull(pipelineMetricPublisher);
  }

  @Test
  public void testAdaptiveInflightLimiterBuilder() {
    Assert.assertNotNull(adaptiveInflightLimiterBuilder);
  }

  @Test
  public void testNode() {
    Assert.assertNotNull(this.node);
    Assert.assertEquals(0, node.getPort());
  }

  @Test
  public void testFilterFactory() {
    Assert.assertNotNull(filterFactory);
    Assert.assertTrue(filterFactory.create(null) instanceof OriginalClusterFilter);
  }
}
