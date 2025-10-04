package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.uber.data.kafka.consumerproxy.utils.RetryUtils;
import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.data.kafka.datatransfer.worker.common.ThreadRegister;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.AbstractKafkaFetcherThread;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcher;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcherConfiguration;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * KafkaFetcherFactory is used to create {@link KafkaFetcher} for {@link
 * com.uber.data.kafka.datatransfer.worker.pipelines.Pipeline}.
 */
public class KafkaFetcherFactory {
  private static final int RESOLVER_CACHE_TTL_MS = 60000;

  private final KafkaFetcherConfiguration config;
  private final LoadingCache<ClusterAndIsSecureKey, String> brokerConnectionStringMap;

  public KafkaFetcherFactory(KafkaFetcherConfiguration config) {
    this.config = config;
    this.brokerConnectionStringMap = createBrokerConnectionStringLoadingCache(config);
  }

  /**
   * Creates a {@link KafkaFetcher} for fetching messages from Kafka servers.
   *
   * @param jobTemplate the template for all jobs that are served by the created {@link
   *     KafkaFetcher}.
   * @param threadId the threadId used by the created {@link KafkaFetcher}.
   * @param coreInfra the core infrastructure
   * @return a {@link KafkaFetcher} for fetching messages from Kafka servers.
   * @throws Exception if there are errors when creating a {@link KafkaFetcher}.
   */
  public KafkaFetcher<byte[], byte[]> create(
      Job jobTemplate, String threadId, ThreadRegister threadRegister, CoreInfra coreInfra)
      throws Exception {
    Scope fetcherScope =
        coreInfra
            .scope()
            .tagged(
                StructuredTags.builder()
                    .setKafkaGroup(jobTemplate.getKafkaConsumerTask().getConsumerGroup())
                    .setKafkaTopic(jobTemplate.getKafkaConsumerTask().getTopic())
                    .setKafkaPartition(jobTemplate.getKafkaConsumerTask().getPartition())
                    .build());
    Stopwatch fetcherCreateTimer = fetcherScope.timer(MetricNames.CREATION_LATENCY).start();
    String topic = jobTemplate.getKafkaConsumerTask().getTopic();
    ClusterAndIsSecureKey clusterAndSecureKey = ClusterAndIsSecureKey.of(jobTemplate);
    if (brokerConnectionStringMap.asMap().containsKey(clusterAndSecureKey)) {
      fetcherScope.counter(MetricNames.RESOLVER_CACHE_HIT).inc(1);
    }
    String bootstrapServers = brokerConnectionStringMap.get(clusterAndSecureKey);
    String consumerGroup = jobTemplate.getKafkaConsumerTask().getConsumerGroup();
    IsolationLevel isolationLevel = jobTemplate.getKafkaConsumerTask().getIsolationLevel();
    AutoOffsetResetPolicy autoOffsetResetPolicy =
        jobTemplate.getKafkaConsumerTask().getAutoOffsetResetPolicy();
    boolean isSecure =
        jobTemplate.hasSecurityConfig() && jobTemplate.getSecurityConfig().getIsSecure();

    Optional<RetryQueue> retryQueue = RetryUtils.findRetryQueueWithTopicName(jobTemplate, topic);

    AbstractKafkaFetcherThread<byte[], byte[]> kafkaFetcher = null;
    // TODO: This is not extensible way of encoding this. The FetcherFactory should make a decision
    // on which type of fetcher to use only using information available in the KafkaConsumerTask.
    // Allow both normal retry as well as tiered to get instance of RetryTopicKafkaFetcher till
    // kcpserver changes are made to populate RetryConfig in Job
    if (topic.equals(jobTemplate.getRpcDispatcherTask().getRetryQueueTopic())
        || retryQueue.isPresent()) {
      kafkaFetcher =
          RetryTopicKafkaFetcher.of(
              threadId,
              bootstrapServers,
              consumerGroup,
              autoOffsetResetPolicy,
              config,
              retryQueue,
              isSecure,
              coreInfra);
    } else if (topic.equals(jobTemplate.getRpcDispatcherTask().getDlqTopic())) {
      kafkaFetcher =
          DlqTopicKafkaFetcher.of(
              threadId, bootstrapServers, consumerGroup, config, isSecure, coreInfra);
    } else {
      kafkaFetcher =
          OriginalTopicKafkaFetcher.of(
              threadId,
              bootstrapServers,
              consumerGroup,
              autoOffsetResetPolicy,
              isolationLevel,
              jobTemplate.getKafkaConsumerTask().getProcessingDelayMs(),
              config,
              isSecure,
              coreInfra);
    }
    KafkaFetcher<byte[], byte[]> fetcher =
        new KafkaFetcher<>(threadRegister.register(kafkaFetcher));
    fetcherCreateTimer.stop();
    return fetcher;
  }

  private LoadingCache<ClusterAndIsSecureKey, String> createBrokerConnectionStringLoadingCache(
      KafkaFetcherConfiguration config) {
    return CacheBuilder.newBuilder()
        .expireAfterWrite(RESOLVER_CACHE_TTL_MS, TimeUnit.MILLISECONDS)
        .build(
            new CacheLoader<>() {
              @Override
              public String load(ClusterAndIsSecureKey clusterAndSecureKey) throws Exception {
                return KafkaUtils.newResolverByClassName(config.getResolverClass())
                    .getBootstrapServers(
                        clusterAndSecureKey.getCluster(),
                        config.getBootstrapServers(),
                        clusterAndSecureKey.isSecure());
              }
            });
  }

  private static class MetricNames {
    static final String RESOLVER_CACHE_HIT = "fetcher.resolver.cache.hit";
    static final String CREATION_LATENCY = "fetcher.create.latency";
  }
}
