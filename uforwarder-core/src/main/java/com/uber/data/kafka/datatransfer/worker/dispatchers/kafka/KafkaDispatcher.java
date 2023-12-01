package com.uber.data.kafka.datatransfer.worker.dispatchers.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.JobUtils;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.instrumentation.Tags;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaDispatcher is an implementation of Dispatcher that produces to Kafka.
 *
 * @implNote this is a functional but low performance implementation.
 */
public final class KafkaDispatcher<K, V> implements Sink<ProducerRecord<K, V>, Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDispatcher.class);
  private static final long FLUSH_INTERVAL_IN_MS = TimeUnit.SECONDS.toMillis(60);
  private final CoreInfra infra;
  private final Producer<K, V> kafkaProducer;
  private final AtomicBoolean running;
  private final ScheduledExecutorService metricsEmitter;
  private final KafkaDispatcherConfiguration configuration;
  private long lastFlushedAt;

  @VisibleForTesting
  KafkaDispatcher(
      CoreInfra infra,
      KafkaDispatcherConfiguration configuration,
      String clientId,
      String cluster,
      boolean isSecure,
      boolean acksOneProducer)
      throws Exception {
    this(
        infra,
        configuration,
        new KafkaProducer<K, V>(
            configuration.getProperties(cluster, clientId, isSecure, acksOneProducer)));
  }

  @VisibleForTesting
  KafkaDispatcher(
      CoreInfra infra, KafkaDispatcherConfiguration configuration, Producer<K, V> kafkaProducer) {
    Stopwatch dispatcherCreationTimer = infra.scope().timer(MetricNames.CREATION_LATENCY).start();
    this.infra = infra;
    this.kafkaProducer = kafkaProducer;
    this.configuration = configuration;
    this.metricsEmitter = infra.contextManager().wrap(Executors.newSingleThreadScheduledExecutor());
    this.running = new AtomicBoolean(false);
    this.lastFlushedAt = -1L;
    dispatcherCreationTimer.stop();
  }

  @Override
  public CompletionStage<Void> submit(ItemAndJob<ProducerRecord<K, V>> request) {
    final Job job = request.getJob();
    final ProducerRecord<K, V> record = request.getItem();
    String topic = record.topic();
    String cluster = JobUtils.getKafkaProducerCluster(job);
    Scope requestScope =
        infra
            .scope()
            .tagged(StructuredTags.builder().setKafkaTopic(topic).setKafkaCluster(cluster).build());
    CompletableFuture<Void> future = new CompletableFuture<>();
    Stopwatch stopwatch = requestScope.timer(MetricNames.SEND_LATENCY).start();

    kafkaProducer.send(
        record,
        (recordMetadata, e) -> {
          stopwatch.stop();
          if (e != null) {
            if (e instanceof RecordTooLargeException) {
              String topicPrefix =
                  "hp-dbevents-schemaless-jobstore-orders-ENTITY_FARES".toLowerCase();
              if (topic.toLowerCase().startsWith(topicPrefix)) {
                requestScope.counter(MetricNames.SKIPPED_LARGE_MESSAGE).inc(1);
                future.complete(null);
                return;
              }
            }
            requestScope
                .tagged(ImmutableMap.of(Tags.Key.code, e.getClass().getSimpleName()))
                .counter(MetricNames.SEND)
                .inc(1);
            LOGGER.error(
                "dispatcher.kafka.send.failure",
                StructuredLogging.kafkaTopic(topic),
                StructuredLogging.kafkaCluster(cluster),
                e);
            future.completeExceptionally(e);
          } else {
            requestScope
                .tagged(ImmutableMap.of(Tags.Key.code, "ok"))
                .counter(MetricNames.SEND)
                .inc(1);
            LOGGER.debug(
                "dispatcher.kafka.send.success",
                StructuredLogging.kafkaTopic(topic),
                StructuredLogging.kafkaCluster(cluster));
            future.complete(null);
          }
        });
    return future;
  }

  /**
   * Flushes the producer
   *
   * @param forceToFlush whether force to flush the producer
   * @return True if flush is done.
   */
  public boolean mayBeFlush(boolean forceToFlush) {
    if (forceToFlush || System.currentTimeMillis() - lastFlushedAt > FLUSH_INTERVAL_IN_MS) {
      kafkaProducer.flush();
      lastFlushedAt = System.currentTimeMillis();
      return true;
    } else {
      return false;
    }
  }

  public void logAndMetrics() {
    Map<MetricName, ? extends Metric> metrics = kafkaProducer.metrics();
    for (Metric metric : metrics.values()) {
      // KafkaProducer.metricValue() returns an untyped Object.
      // In 2.2.1, the Object can be a generic type T or a Double:
      // kafka/clients/src/main/java/org/apache/kafka/common/metrics/KafkaMetric.java#L65
      // kafka/clients/src/main/java/org/apache/kafka/common/metrics/Measurable.java#L30
      // We report only the Double metrics since we have no way of determining the type parameter T.
      if (metric.metricValue() instanceof Double) {
        infra
            .scope()
            .tagged(metric.metricName().tags())
            .gauge(metric.metricName().name())
            .update((Double) metric.metricValue());
        LOGGER.debug(
            "emit KafkaProducer metric", StructuredLogging.metricName(metric.metricName().name()));
      } else {
        LOGGER.debug(
            "skipping KafkaProducer metric",
            StructuredLogging.reason("type not Double"),
            StructuredLogging.metricName(metric.metricName().name()));
      }
    }
  }

  @Override
  public void start() {
    metricsEmitter.scheduleAtFixedRate(
        this::logAndMetrics, 0, configuration.getMetricsInterval(), TimeUnit.MILLISECONDS);
    running.set(true);
  }

  @Override
  public void stop() {
    kafkaProducer.close();
    metricsEmitter.shutdownNow();
    running.set(false);
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  private static class MetricNames {
    static final String SEND = "dispatcher.kafka.send";
    static final String SEND_LATENCY = "dispatcher.kafka.send.latency";
    static final String SKIPPED_LARGE_MESSAGE = "dispatcher.kafka.skipped_large_message";
    static final String CREATION_LATENCY = "kafka.dispatcher.create.latency";
  }
}
