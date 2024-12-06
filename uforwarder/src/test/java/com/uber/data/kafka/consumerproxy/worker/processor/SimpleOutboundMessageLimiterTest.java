package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.worker.limiter.AdaptiveInflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.limiter.InflightLimiter;
import com.uber.data.kafka.consumerproxy.worker.limiter.VegasAdaptiveInflightLimiter;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.context.ContextManager;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class SimpleOutboundMessageLimiterTest extends ProcessorTestBase {

  private Job job1;
  private SimpleOutboundMessageLimiter outboundMessageLimiter;
  private Gauge inflight;
  private Gauge oneMinuteMaxInflight;
  private Gauge oneMinuteMinInflight;
  private Gauge limit;
  private Gauge adaptiveLimit;
  private Gauge shadowAdaptiveLimit;
  private Gauge queueSize;
  private CoreInfra infra;
  private ProcessorMessage pm1;
  private TopicPartition tp1;
  private AdaptiveInflightLimiter.Builder adaptiveInflightLimiterBuilder;
  private AdaptiveInflightLimiter adaptiveInflightLimiter;

  @Before
  public void setUp() throws Exception {
    tp1 = new TopicPartition("topic", 1);
    pm1 = newProcessMessage(new TopicPartitionOffset(tp1.topic(), tp1.partition(), 0));
    job1 =
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster("a")
                    .setConsumerGroup("b")
                    .setTopic(tp1.topic())
                    .setPartition(tp1.partition())
                    .build())
            .build();
    Scope scope = Mockito.mock(Scope.class);
    Counter counter = Mockito.mock(Counter.class);
    inflight = Mockito.mock(Gauge.class);
    limit = Mockito.mock(Gauge.class);
    adaptiveLimit = Mockito.mock(Gauge.class);
    shadowAdaptiveLimit = Mockito.mock(Gauge.class);
    ContextManager contextManager = Mockito.mock(ContextManager.class);
    Mockito.when(contextManager.wrap(Mockito.any(CompletableFuture.class)))
        .thenAnswer(
            (Answer<CompletableFuture>)
                invocation -> invocation.getArgument(0, CompletableFuture.class));
    infra = CoreInfra.builder().withScope(scope).withContextManager(contextManager).build();
    oneMinuteMaxInflight = Mockito.mock(Gauge.class);
    oneMinuteMinInflight = Mockito.mock(Gauge.class);
    queueSize = Mockito.mock(Gauge.class);
    Mockito.when(contextManager.wrap(Mockito.any(ExecutorService.class)))
        .thenAnswer(
            (Answer<ExecutorService>)
                invocation -> invocation.getArgument(0, ExecutorService.class));
    adaptiveInflightLimiterBuilder = Mockito.mock(AdaptiveInflightLimiter.Builder.class);
    Mockito.when(adaptiveInflightLimiterBuilder.withLogEnabled(Mockito.anyBoolean()))
        .thenReturn(adaptiveInflightLimiterBuilder);
    adaptiveInflightLimiter = Mockito.spy(VegasAdaptiveInflightLimiter.newBuilder().build());
    Mockito.when(adaptiveInflightLimiterBuilder.build()).thenReturn(adaptiveInflightLimiter);
    Mockito.when(scope.subScope(ArgumentMatchers.anyString())).thenReturn(scope);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge("processor.outbound-cache.size")).thenReturn(inflight);
    Mockito.when(scope.gauge("processor.outbound-cache.size.one-minute-max"))
        .thenReturn(oneMinuteMaxInflight);
    Mockito.when(scope.gauge("processor.outbound-cache.size.one-minute-min"))
        .thenReturn(oneMinuteMinInflight);
    Mockito.when(scope.gauge("processor.outbound-cache.limit")).thenReturn(limit);
    Mockito.when(scope.gauge("processor.outbound-cache.adaptive-limit")).thenReturn(adaptiveLimit);
    Mockito.when(scope.gauge("processor.outbound-cache.shadow-adaptive-limit"))
        .thenReturn(shadowAdaptiveLimit);
    Mockito.when(scope.gauge("processor.outbound-cache.queue")).thenReturn(queueSize);
    mockMetrics(scope, "processor.outbound-cache.adaptive-limit");
    mockMetrics(scope, "processor.outbound-cache.shadow-adaptive-limit");
    outboundMessageLimiter =
        (SimpleOutboundMessageLimiter)
            (new SimpleOutboundMessageLimiter.Builder(infra, adaptiveInflightLimiterBuilder, false)
                    .withMaxOutboundCacheCount(800))
                .build(job1);
    outboundMessageLimiter.updateLimit(2);
    outboundMessageLimiter.init(job1);
  }

  private void mockMetrics(Scope scope, String prefix) {
    Mockito.when(scope.gauge(ArgumentMatchers.startsWith(prefix + ".")))
        .thenReturn(Mockito.mock(Gauge.class));
  }

  @Test
  public void testAcquire() throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> permit =
        outboundMessageLimiter.acquirePermitAsync(pm1);
    Assert.assertNotNull(permit.get());
  }

  @Test
  public void testComplete() throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> permit =
        outboundMessageLimiter.acquirePermitAsync(pm1);
    permit.get().complete(InflightLimiter.Result.Succeed);
  }

  @Test
  public void testUpdateLimit() throws ExecutionException, InterruptedException {
    outboundMessageLimiter.acquirePermit(pm1);
    outboundMessageLimiter.acquirePermit(pm1);
    outboundMessageLimiter.updateLimit(3);
    CompletableFuture<InflightLimiter.Permit> permit =
        outboundMessageLimiter.acquirePermitAsync(pm1);
    permit.get().complete(InflightLimiter.Result.Succeed);
    outboundMessageLimiter.updateLimit(2);
  }

  @Test
  public void testUpdateLimitWhileBlocked() {
    // test update limit won't be blocked
    AtomicInteger sequence = new AtomicInteger(0);
    long start = System.currentTimeMillis();
    outboundMessageLimiter.acquirePermit(pm1);
    outboundMessageLimiter.acquirePermit(pm1);
    Executors.newSingleThreadScheduledExecutor()
        .schedule(
            () -> {
              sequence.incrementAndGet();
              outboundMessageLimiter.updateLimit(3); // unblock
            },
            100,
            TimeUnit.MILLISECONDS);
    outboundMessageLimiter.acquirePermit(pm1);
    Assert.assertEquals(1, sequence.get());

    Executors.newSingleThreadScheduledExecutor()
        .schedule(
            () -> {
              sequence.incrementAndGet();
              outboundMessageLimiter.updateLimit(2); // not being blocked
              sequence.incrementAndGet();
              outboundMessageLimiter.updateLimit(4); // unblock
            },
            100,
            TimeUnit.MILLISECONDS);
    outboundMessageLimiter.acquirePermit(pm1);
    Assert.assertEquals(3, sequence.get());
  }

  @Test
  public void testClose() {
    outboundMessageLimiter.acquirePermit(pm1);
    outboundMessageLimiter.close();
  }

  @Test
  public void testPublishMetrics() throws ExecutionException, InterruptedException {
    CompletableFuture<InflightLimiter.Permit> permit =
        outboundMessageLimiter.acquirePermitAsync(pm1);
    outboundMessageLimiter.publishMetrics();
    Mockito.verify(limit).update(2.0);
    Mockito.verify(oneMinuteMinInflight).update(0.0);
    Mockito.verify(oneMinuteMaxInflight).update(1.0);
    Mockito.verify(inflight).update(1.0);
    Mockito.verify(adaptiveLimit).update(100.0);
    Mockito.verify(shadowAdaptiveLimit).update(100.0);
    Mockito.verify(queueSize).update(0.0);
    permit.get().complete(InflightLimiter.Result.Succeed);
    Mockito.reset(
        limit,
        inflight,
        adaptiveLimit,
        shadowAdaptiveLimit,
        oneMinuteMaxInflight,
        oneMinuteMinInflight,
        queueSize);

    outboundMessageLimiter.publishMetrics();
    Mockito.verify(limit).update(2.0);
    Mockito.verify(oneMinuteMinInflight).update(0.0);
    Mockito.verify(oneMinuteMaxInflight).update(1.0);
    Mockito.verify(inflight).update(0.0);
    Mockito.verify(adaptiveLimit).update(100.0);
    Mockito.verify(shadowAdaptiveLimit).update(100.0);
    Mockito.verify(queueSize).update(0.0);
    outboundMessageLimiter.cancel(job1);
    Mockito.reset(
        limit,
        inflight,
        adaptiveLimit,
        shadowAdaptiveLimit,
        oneMinuteMaxInflight,
        oneMinuteMinInflight);

    outboundMessageLimiter.publishMetrics();
    Mockito.verify(limit, Mockito.never()).update(Mockito.anyDouble());
    Mockito.verify(inflight, Mockito.never()).update(Mockito.anyDouble());
    Mockito.verify(adaptiveLimit, Mockito.never()).update(Mockito.anyDouble());
  }

  @Test
  public void testLimiterFunc() {
    SimpleOutboundMessageLimiter.LimiterFunc func =
        outboundMessageLimiter.new LimiterFunc(l -> l * 2);
    func.setDryRun(false);
    Assert.assertEquals(0.2, func.apply(0.1), 0.001);
  }

  @Test(expected = IllegalStateException.class)
  public void testAcquireBeforeInit() throws Exception {
    ProcessorMessage pm2 = newProcessMessage(new TopicPartitionOffset("other-topic", 0, 0));
    outboundMessageLimiter.acquirePermit(pm2);
  }

  @Test(expected = IllegalStateException.class)
  public void testAcquireAfterCancel() {
    outboundMessageLimiter.cancelAll();
    outboundMessageLimiter.acquirePermit(pm1);
  }

  @Test
  public void testJobs() {
    Collection<Job> jobs = outboundMessageLimiter.jobs();
    Assert.assertTrue(jobs.contains(job1));
  }

  @Test
  public void testCancelJobUpdateMaxInflight() {
    // 2 updates one for primary another for shadow limiter
    Mockito.verify(adaptiveInflightLimiter, Mockito.times(2)).setMaxInflight(1000);
    Mockito.reset(adaptiveInflightLimiter);
    outboundMessageLimiter.cancel(job1);
    Mockito.verify(adaptiveInflightLimiter, Mockito.times(2)).setMaxInflight(1000);
  }

  @Test
  public void testInitJobUpdateMaxInflight() {
    // 2 updates one for primary another for shadow limiter
    Mockito.verify(adaptiveInflightLimiter, Mockito.times(2)).setMaxInflight(1000);
    Mockito.reset(adaptiveInflightLimiter);
    outboundMessageLimiter.init(
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster("a")
                    .setConsumerGroup("b")
                    .setTopic("topic")
                    .setPartition(2)
                    .build())
            .build());
    Mockito.verify(adaptiveInflightLimiter, Mockito.times(2)).setMaxInflight(1600);
  }
}
