package com.uber.data.kafka.datatransfer.worker.dispatchers.kafka;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class KafkaDispatcherTest extends FievelTestBase {
  private KafkaDispatcher kafkaDispatcher;
  private MockProducer mockKafkaProducer;
  private Scope mockScope;
  private Counter mockCounter;
  private Gauge mockGauge;
  private Timer mockTimer;
  private Stopwatch mockStopwatch;

  @Before
  public void setup() {
    mockScope = mock(Scope.class);
    mockCounter = mock(Counter.class);
    mockGauge = mock(Gauge.class);
    mockTimer = mock(Timer.class);
    mockStopwatch = mock(Stopwatch.class);
    when(mockScope.subScope(anyString())).thenReturn(mockScope);
    when(mockScope.tagged(any())).thenReturn(mockScope);
    when(mockScope.counter(any())).thenReturn(mockCounter);
    when(mockScope.gauge(any())).thenReturn(mockGauge);
    when(mockScope.timer(any())).thenReturn(mockTimer);
    when(mockTimer.start()).thenReturn(mockStopwatch);
    mockKafkaProducer =
        new MockProducer(false, Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer());
    CoreInfra infra = CoreInfra.builder().withScope(mockScope).build();
    kafkaDispatcher =
        new KafkaDispatcher(infra, new KafkaDispatcherConfiguration(), mockKafkaProducer);
  }

  @Test
  public void testProduceSuccess() throws Exception {
    CompletableFuture future =
        kafkaDispatcher
            .submit(
                ItemAndJob.of(
                    new ProducerRecord("test-topic", "test-data".getBytes()),
                    Job.newBuilder().build()))
            .toCompletableFuture();
    mockKafkaProducer.completeNext();
    Assert.assertEquals(1, mockKafkaProducer.history().size());
    future.get(1, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void testProduceFailure() throws Exception {
    CompletableFuture future =
        kafkaDispatcher
            .submit(
                ItemAndJob.of(
                    new ProducerRecord("test-topic", "test-data".getBytes()),
                    Job.newBuilder().build()))
            .toCompletableFuture();
    mockKafkaProducer.errorNext(new IllegalStateException());
    Assert.assertEquals(1, mockKafkaProducer.history().size());
    future.get(1, TimeUnit.SECONDS);
  }

  @Test
  public void testProducerFlushWhenForceToFlushIsFalse() throws Exception {
    Assert.assertTrue(kafkaDispatcher.mayBeFlush(false));
    Assert.assertFalse(kafkaDispatcher.mayBeFlush(false));
  }

  @Test
  public void testProducerFlushWhenForceToFlushIsTrue() throws Exception {
    Assert.assertTrue(kafkaDispatcher.mayBeFlush(false));
    Assert.assertTrue(kafkaDispatcher.mayBeFlush(true));
  }

  @Test
  public void testMetricsReporter() throws Exception {
    MetricName metricName =
        new MetricName("metric-name", "metric-group", "metric-description", ImmutableMap.of());
    Metric metric = mock(Metric.class);
    when(metric.metricName()).thenReturn(metricName);
    when(metric.metricValue()).thenReturn(0.0);
    mockKafkaProducer.setMockMetrics(metricName, metric);
    kafkaDispatcher.logAndMetrics();
    verify(mockGauge, atLeastOnce()).update(0.0);
  }

  @Test
  public void testMetricsReporterNonDoubleMetric() throws Exception {
    MetricName metricName =
        new MetricName("metric-name", "metric-group", "metric-description", ImmutableMap.of());
    Metric metric = mock(Metric.class);
    when(metric.metricName()).thenReturn(metricName);
    when(metric.metricValue()).thenReturn("not-double");
    mockKafkaProducer.setMockMetrics(metricName, metric);
    kafkaDispatcher.logAndMetrics();
    verify(mockGauge, never()).update(Mockito.anyDouble());
  }

  @Test
  public void testLifecycle() {
    kafkaDispatcher.start();
    Assert.assertTrue(kafkaDispatcher.isRunning());
    kafkaDispatcher.stop();
  }
}
