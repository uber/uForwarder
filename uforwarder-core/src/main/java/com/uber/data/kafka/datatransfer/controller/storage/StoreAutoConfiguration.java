package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.StoredJobStatus;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.CoreInfraAutoConfiguration;
import com.uber.data.kafka.datatransfer.common.ReadStore;
import com.uber.data.kafka.datatransfer.common.ZKUtils;
import com.uber.data.kafka.datatransfer.controller.config.JobGroupStoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.config.JobStatusStoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.config.JobStoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.config.StoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.config.WorkerStoreConfiguration;
import com.uber.data.kafka.datatransfer.controller.config.ZookeeperConfiguration;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import java.util.function.Function;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * This is a factory for objects in this package.
 *
 * <p>For each item to be constructed, there are two construction methods:
 *
 * <ul>
 *   <li>Spring friendly provider, named {@code getX}, that returns spring {@code Bean}. This is
 *       automatically used in spring framework
 *   <li>Static factory method, named {@code buildX}, that returns the object for use outside of
 *       spring framework.
 * </ul>
 */
@Configuration
@ConditionalOnProperty(
    prefix = "master.store",
    name = "enabled",
    havingValue = "true",
    matchIfMissing = true)
@EnableConfigurationProperties({
  JobStoreConfiguration.class,
  JobGroupStoreConfiguration.class,
  JobStatusStoreConfiguration.class,
  WorkerStoreConfiguration.class,
  ZookeeperConfiguration.class,
})
@Import({CoreInfraAutoConfiguration.class, TypeRegistryAutoConfiguration.class})
@Profile("data-transfer-controller")
@EnableScheduling
public class StoreAutoConfiguration {

  @Bean
  public ReadStore<Long, StoredJob> jobStore(Store<String, StoredJobGroup> jobGroupStore) {
    return new JobStore(jobGroupStore);
  }

  @Bean
  public Store<String, StoredJobGroup> jobGroupStore(
      JobGroupStoreConfiguration config,
      ZookeeperConfiguration zkConfiguration,
      CoreInfra infra,
      JsonFormat.TypeRegistry typeRegistry,
      IdProvider<String, StoredJobGroup> idProvider,
      LeaderSelector leaderSelector)
      throws Exception {
    return getZkStore(
        config,
        zkConfiguration.getZkConnection(),
        infra,
        StoredJobGroup.newBuilder().build(),
        typeRegistry,
        j -> j.getJobGroup().getJobGroupId(),
        idProvider,
        leaderSelector);
  }

  @Bean
  public Store<Long, StoredJobStatus> jobStatusStore(
      JobStatusStoreConfiguration jobStatusConfig,
      ZookeeperConfiguration zkConfiguration,
      CoreInfra infra,
      JsonFormat.TypeRegistry typeRegistry,
      IdProvider<Long, StoredJobStatus> idProvider,
      LeaderSelector leaderSelector)
      throws Exception {
    return new LocalStore<>(idProvider);
  }

  @Bean
  public Store<Long, StoredWorker> workerStore(
      WorkerStoreConfiguration workerConfig,
      ZookeeperConfiguration zkConfiguration,
      CoreInfra infra,
      JsonFormat.TypeRegistry typeRegistry,
      IdProvider<Long, StoredWorker> idProvider,
      LeaderSelector leaderSelector)
      throws Exception {
    return getZkStore(
        workerConfig,
        zkConfiguration.getZkConnection(),
        infra,
        StoredWorker.newBuilder().build(),
        typeRegistry,
        w -> w.getNode().getId(),
        idProvider,
        leaderSelector);
  }

  @Bean
  public IdProvider<Long, StoredJob> jobIdProvider(
      JobStoreConfiguration config, ZookeeperConfiguration zkConfiguration) throws Exception {
    return getZKSequencer(config, zkConfiguration.getZkConnection());
  }

  @Bean
  public IdProvider<Long, StoredWorker> workerIdProvider(
      WorkerStoreConfiguration config, ZookeeperConfiguration zkConfiguration) throws Exception {
    return getZKSequencer(config, zkConfiguration.getZkConnection());
  }

  @Bean
  public IdProvider<Long, StoredJobStatus> jobStatusIdProvider() throws Exception {
    return new IdExtractor<>(j -> j.getJobStatus().getJob().getJobId());
  }

  @Bean
  public IdProvider<String, StoredJobGroup> jobGroupIdProvider() {
    return new JobGroupIdProvider();
  }

  private <V extends Message> IdProvider<Long, V> getZKSequencer(
      StoreConfiguration config, String zkConnection) throws Exception {
    Preconditions.checkArgument(config.getMode() == Mode.ZK, "must run Mode.ZK in production");
    CuratorFramework curatorFramework = ZKUtils.getCuratorFramework(zkConnection);
    return new ZKSequencer<>(curatorFramework, config.getZkSequencerPath());
  }

  protected <K, V extends Message> Store<K, V> getZkStore(
      StoreConfiguration config,
      String zkConnection,
      CoreInfra infra,
      V prototype,
      JsonFormat.TypeRegistry typeRegistry,
      Function<V, K> keyFn,
      IdProvider<K, V> idProvider,
      LeaderSelector leaderSelector)
      throws Exception {
    Logger logger = LoggerFactory.getLogger("store." + prototype.getDescriptorForType().getName());
    CoreInfra storeTypeInfra =
        infra.subScope("store." + prototype.getDescriptorForType().getName());
    Preconditions.checkArgument(config.getMode() == Mode.ZK, "must run Mode.ZK in production");
    CuratorFramework curatorFramework = ZKUtils.getCuratorFramework(zkConnection);
    SerializerType serializerType = config.getSerializerType();
    Store<K, V> store =
        new ZKStore<>(
            logger,
            storeTypeInfra,
            AsyncCuratorFramework.wrap(curatorFramework),
            serializerType.getSerializer(prototype, typeRegistry),
            idProvider,
            keyFn,
            config.getZkDataPath());

    // ZKStore doesn't provide strong read-after-write consistency:
    // 1) There is no method to check whether the given element exists or not in the store.
    // 2) The getAll() method after the create/put operation may return empty.
    // 3) To perform upsert operation, two operations are required: get() if element exists, then
    // use #put() else #create(). The get() operation throws NoSuchElementException if the element
    // doesn't exist.
    // 4) LoggingAndMetricsDecorator logs all the exceptions by default even-though they are
    // ignored. This pollutes the log statements when #get() is used to perform upsert operation
    // and can hide the real exceptions. So, made the LoggingAndMetricsDecorator as optional.
    if (config.isLogDecoratorEnabled()) {
      // LoggingAndMetricsDecorator internally emits metrics using "prototype.method" so we only
      // subscope "store".
      // It does so b/c it needs to emit "prototype.method" for jaeger trace.
      // Put it before BufferedWriteDecorator so that we can report store metrics accurately without
      // the interference from buffered write
      store =
          LoggingAndMetricsStoreDecorator.decorate(
              prototype, logger, infra.subScope("store"), store);
    }

    if (config.getBufferedWriteInterval().toMillis() > 0) {
      store =
          BufferedWriteDecorator.decorate(
              config.getBufferedWriteInterval(), logger, storeTypeInfra, store);
    }

    if (!config.getTtl().isZero()) {
      store = TTLDecorator.decorate(storeTypeInfra, store, keyFn, config.getTtl(), leaderSelector);
    }

    return store;
  }
}
