package com.uber.data.kafka.consumerproxy.controller;

import com.uber.data.kafka.clients.admin.MultiClusterAdmin;
import com.uber.data.kafka.consumerproxy.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.data.kafka.datatransfer.Node;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import com.uber.data.kafka.datatransfer.common.KafkaPartitionExpansionWatcher;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.creator.JobCreator;
import com.uber.data.kafka.datatransfer.controller.rebalancer.Rebalancer;
import com.uber.data.kafka.datatransfer.controller.rebalancer.ShadowRebalancerDelegate;
import com.uber.data.kafka.datatransfer.controller.storage.Store;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {UForwarderControllerFactory.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
@ActiveProfiles({"data-transfer-controller", "uforwarder-controller"})
public class UForwarderControllerFactoryTest extends FievelTestBase {

  @MockBean LeaderSelector leaderSelector;

  @MockBean Store<String, StoredJobGroup> jobGroupStore;

  @MockBean Store<Long, StoredJobStatus> jobStatusStore;

  @MockBean Store<Long, StoredWorker> workerStore;

  @Autowired private DynamicConfiguration dynamicConfiguration;

  @Autowired private Rebalancer rebalancer;

  @Autowired private JobCreator jobCreator;

  @Autowired private MultiClusterAdmin multiClusterAdmin;

  @Autowired private KafkaPartitionExpansionWatcher kafkaPartitionExpansionWatcher;

  @Autowired private Node node;

  @Autowired private ShadowRebalancerDelegate shadowRebalancerDelegate;

  @Test
  public void testDynamicConfiguration() {
    Assert.assertNotNull(this.dynamicConfiguration);
  }

  @Test
  public void testRebalancer() {
    Assert.assertNotNull(this.rebalancer);
  }

  @Test
  public void testJobCreator() {
    Assert.assertNotNull(this.jobCreator);
  }

  @Test
  public void testMultiClusterAdmin() {
    Assert.assertNotNull(this.multiClusterAdmin);
  }

  @Test
  public void testKafkaPartitionExpansionWatcher() {
    Assert.assertNotNull(this.kafkaPartitionExpansionWatcher);
  }

  @Test
  public void testNode() {
    Assert.assertNotNull(this.node);
    Assert.assertEquals(8081, node.getPort());
  }

  @Test
  public void testShadowRebalancerDelegate() {
    Assert.assertNotNull(this.shadowRebalancerDelegate);
  }
}
