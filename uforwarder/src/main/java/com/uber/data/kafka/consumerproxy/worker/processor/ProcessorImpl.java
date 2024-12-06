package com.uber.data.kafka.consumerproxy.worker.processor;

import static com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse.responseCodeDistributionBuckets;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.utils.RetryUtils;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherMessage;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.consumerproxy.worker.filter.Filter;
import com.uber.data.kafka.consumerproxy.worker.limiter.InflightLimiter;
import com.uber.data.kafka.datatransfer.CommandType;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.RoutingUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.datatransfer.worker.common.Chainable;
import com.uber.data.kafka.datatransfer.worker.common.Configurable;
import com.uber.data.kafka.datatransfer.worker.common.Controllable;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.instrumentation.DirectSupplier;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.data.kafka.instrumentation.Tags;
import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.DurationBuckets;
import com.uber.m3.tally.Histogram;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.util.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.Fallback;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedSupplier;
import net.jodah.failsafe.util.concurrent.Scheduler;
import net.logstash.logback.argument.StructuredArgument;
import net.logstash.logback.argument.StructuredArguments;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProcessorImpl is the brain of the consumer proxy. It controls message commit, message prefetch
 * and message dispatch.
 *
 * <p>the high-level workflow is
 *
 * <pre>
 *                 fetcher
 *                  | | |
 *                  | | |    fetcher threads submit to processor
 *                  | | |
 *           convert to kafka message
 *                  | | |
 *                  | | |    submit to (it's run in fetcher thread)
 *                  \ | /
 * unprocessed message queue (UnprocessedMessageManager it might be blocked by a semaphore)
 *                    |
 *                    |      a single thread submits to dispatcher queue
 *                    |      this single thread is dedicated to the pipeline
 *                    |
 * outbound message queue (OutboundCacheSemaphoreAndExecutor it might be blocked by a semaphore.
 *                          The outbound message queue is necessary because the rate limiter
 *                          provided by the KafkaFetcher is not enough. We not only want to limit
 *                          message rate, but also want to limit the outbound message count, so we
 *                          don't flood the recipients when it cannot process messages)
 *
 *                  / | \
 *                  | | |    multiple threads to dispatch messages
 *                  | | |
 *             dispatching logic
 *                  | | |
 *                  | | |    the same set of threads
 *                  | | |
 *    remove from outbound message queue
 *                  | | |
 *                  | | |    the same set of threads
 *                  | | |
 *   remove from unprocessed message queue
 *                  | | |
 *                  | | |    the same set of threads
 *                  | | |
 *         determine commit status
 * </pre>
 *
 * Following the MVCS (https://eng.uberinternal.com/docs/glue/mvcs) system design, Processor maps to
 * the "Service Layer", which should not directly interact with gateway and repository.
 */
public class ProcessorImpl
    implements Sink<ConsumerRecord<byte[], byte[]>, Long>,
        Chainable<DispatcherMessage, DispatcherResponse>,
        Controllable,
        Configurable,
        MetricSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorImpl.class);
  private static final double MINIMUM_VALID_RATE = 1.0;

  private static final Buckets E2E_DURATION_BUCKETS =
      new DurationBuckets(
          new Duration[] {
            Duration.ZERO,
            Duration.ofMillis(10),
            Duration.ofMillis(12),
            Duration.ofMillis(14),
            Duration.ofMillis(16),
            Duration.ofMillis(18),
            Duration.ofMillis(20),
            Duration.ofMillis(25),
            Duration.ofMillis(30),
            Duration.ofMillis(35),
            Duration.ofMillis(40),
            Duration.ofMillis(45),
            Duration.ofMillis(50),
            Duration.ofMillis(60),
            Duration.ofMillis(70),
            Duration.ofMillis(80),
            Duration.ofMillis(90),
            Duration.ofMillis(100),
            Duration.ofMillis(150),
            Duration.ofMillis(200),
            Duration.ofMillis(250),
            Duration.ofMillis(300),
            Duration.ofMillis(350),
            Duration.ofMillis(400),
            Duration.ofMillis(450),
            Duration.ofMillis(500),
            Duration.ofMillis(600),
            Duration.ofMillis(700),
            Duration.ofMillis(800),
            Duration.ofMillis(900),
            Duration.ofMillis(1000),
            Duration.ofMillis(1250),
            Duration.ofMillis(1500),
            Duration.ofMillis(1750),
            Duration.ofMillis(2000),
            Duration.ofMillis(2250),
            Duration.ofMillis(2500),
            Duration.ofMillis(3000),
            Duration.ofMillis(3500),
            Duration.ofMillis(4000),
            Duration.ofMillis(4500),
            Duration.ofMillis(5000),
            Duration.ofSeconds(6),
            Duration.ofSeconds(7),
            Duration.ofSeconds(8),
            Duration.ofSeconds(9),
            Duration.ofSeconds(10),
            Duration.ofSeconds(20),
            Duration.ofSeconds(30),
            Duration.ofSeconds(40),
            Duration.ofSeconds(50),
            Duration.ofSeconds(60),
            Duration.ofMinutes(2),
            Duration.ofMinutes(4),
            Duration.ofMinutes(8),
            Duration.ofMinutes(16),
            Duration.ofMinutes(32),
            Duration.ofMinutes(64),
            Duration.ofHours(2),
            Duration.ofHours(4),
            Duration.ofHours(8),
            Duration.ofHours(16),
            Duration.ofHours(32)
          });
  @VisibleForTesting final RateLimiter messageRateLimiter;
  @VisibleForTesting final RateLimiter byteRateLimiter;
  @VisibleForTesting final DlqDispatchManager dlqDispatchManager;

  // make it volatile so when one thread set it, other thread will see it.
  @Nullable private volatile Sink<DispatcherMessage, DispatcherResponse> messageDispatcher;
  @Nullable private volatile PipelineStateManager pipelineStateManager;
  private final ScheduledExecutorService executor;
  private final AckManager ackManager;
  private final OutboundMessageLimiter outboundMessageLimiter;
  private final CoreInfra infra;
  private final AtomicBoolean isRunning;

  private final int maxGrpcRetry;
  private final int maxKafkaRetry;

  private final String addressFromUri;

  private final Filter messageFilter;

  public ProcessorImpl(
      Job job,
      ScheduledExecutorService executor,
      OutboundMessageLimiter.Builder outboundMessageLimiterBuilder,
      MessageAckStatusManager.Builder ackStatusManagerBuilder,
      UnprocessedMessageManager.Builder unprocessedManagerBuilder,
      Filter messageFilter,
      int outboundMessageLimit,
      CoreInfra infra) {
    // create a default message dispatcher to avoid NullPointerException if setNextStage is invoked
    // after the worker starts.
    this(
        new AckManager(
            ackStatusManagerBuilder.build(job),
            unprocessedManagerBuilder.build(job),
            infra.scope()),
        executor, // this executor is for non-blocking operations
        outboundMessageLimiterBuilder.build(job),
        job.getRpcDispatcherTask().getUri(),
        outboundMessageLimit,
        messageFilter,
        // TODO(haitao.zhang): make those two values configurable
        Integer.MAX_VALUE,
        Integer.MAX_VALUE,
        infra);
  }

  @VisibleForTesting
  ProcessorImpl(
      AckManager ackManager,
      ScheduledExecutorService executor,
      OutboundMessageLimiter outboundMessageLimiter,
      String jobUri,
      int outboundMessageLimit,
      Filter messageFilter,
      int maxGrpcRetry,
      int maxKafkaRetry,
      CoreInfra infra) {
    this.ackManager = ackManager;
    this.executor = executor;
    this.outboundMessageLimiter = outboundMessageLimiter;
    this.addressFromUri = RoutingUtils.extractAddress(jobUri);
    this.maxGrpcRetry = maxGrpcRetry;
    this.messageFilter = messageFilter;
    this.maxKafkaRetry = maxKafkaRetry;
    this.infra = infra;
    this.isRunning = new AtomicBoolean(false);
    this.messageRateLimiter = RateLimiter.create(MINIMUM_VALID_RATE);
    this.byteRateLimiter = RateLimiter.create(MINIMUM_VALID_RATE);
    this.dlqDispatchManager = new DlqDispatchManager(infra.scope());
    this.outboundMessageLimiter.updateLimit(outboundMessageLimit);
  }

  public Map<Job, Map<Long, MessageStub>> getStubs() {
    return ackManager.getStubs();
  }

  /**
   * Creates a CheckedSupplier that invokes the gRPC dispatcher.
   *
   * @param record to send to dispatcher.
   * @return CheckedSupplier that can be invoked on Jodah Failsafe.
   */
  private CheckedSupplier<CompletionStage<DispatcherResponseAndOffset>> dispatchToGrpcOutbound(
      ItemAndJob<ProcessorMessage> record, String[] tags) {
    return () ->
        Instrumentation.instrument.withRuntimeException(
            LOGGER,
            infra.scope(),
            () -> {
              final ProcessorMessage processorMessage = record.getItem();
              final Job job = record.getJob();
              // skip dispatching as job has been cancelled
              if (!outboundMessageLimiter.contains(job)) {
                return CompletableFuture.completedFuture(
                    new DispatcherResponseAndOffset(DispatcherResponse.Code.SKIP, -1));
              }

              final Job finalJob = getJob(job.getJobId(), job);
              // GRPC dispatch byte rate
              Scope subScope = infra.scope().tagged(getMetricsTags(finalJob));
              subScope.counter(MetricNames.BYTES_RATE).inc(processorMessage.getValueByteSize());
              // The response distribution is used to calculate the distribution of response codes.
              // It leverages the Histogram metrics so that the distribution will be pre-aggregated
              // instead of us aggregating it in M3. It helps to reduce the metrics, and more
              // importantly, allows us to count how many consumers are seeing 100% DLQ failures.
              // Example: by using the `histogramCDF bucketid bucket 1` function, the cumulated
              //          density of values in [0, 1) will be calculated, this essentially means the
              //          probability of DLQ failure in this case.
              Histogram responseDistribution =
                  subScope.histogram(
                      MetricNames.RESPONSE_DISTRIBUTION, responseCodeDistributionBuckets);

              // Activate the span for dispatch.
              // The span(s) will track gRPC calls and retries.
              // Scope will be closed automatically on concurrent thread to prevent memory leak.
              // memory leak could happen if nested span created on opened span because of context
              // propagation
              return cancelSafeStage(
                  Instrumentation.instrument
                      .withExceptionalCompletion(
                          LOGGER,
                          infra.scope(),
                          () -> {
                            // block on the dispatch semaphore, which limits the concurrency of
                            // gRPC dispatches.
                            return outboundMessageLimiter.acquirePermitAsync(processorMessage);
                          },
                          "processor.prefetch.outbound-cache.insert",
                          tags)
                      .thenComposeAsync(
                          permit ->
                              doDispatch(processorMessage, permit, finalJob, responseDistribution),
                          executor));
            },
            "processor.dispatch.grpc",
            tags);
  }

  private CompletionStage<DispatcherResponseAndOffset> doDispatch(
      ProcessorMessage processorMessage,
      InflightLimiter.Permit permit,
      Job finalJob,
      Histogram responseDistribution) {
    Preconditions.checkNotNull(messageDispatcher, "message dispatcher must not be null");
    try (io.opentracing.Scope ignoredScope =
        infra.tracer().activateSpan(processorMessage.getSpan())) {
      return messageDispatcher
          .submit(
              ItemAndJob.of(
                  processorMessage.getGrpcDispatcherMessage(
                      finalJob.getRpcDispatcherTask().getUri()),
                  finalJob))
          .whenComplete(
              (r, e) -> {
                Scope subScope = infra.scope().tagged(getMetricsTags(finalJob));
                if (r != null) {
                  responseDistribution.recordValue(r.getCode().ordinal());
                  if (r.getCode() == DispatcherResponse.Code.COMMIT) {
                    // measure message end-to-end latency
                    subScope
                        .histogram(MetricNames.MESSAGE_END_TO_END_LATENCY, E2E_DURATION_BUCKETS)
                        .recordDuration(
                            Duration.ofMillis(
                                System.currentTimeMillis()
                                    - processorMessage.getLogicalTimestamp()));
                  }
                }
              })
          .whenComplete((r, e) -> handlePermit(r, e, permit))
          .thenApply(r -> handleTimeout(r, processorMessage, finalJob))
          .thenApply(
              r -> {
                switch (r.getCode()) {
                  case SKIP:
                    // fallthrough
                  case COMMIT:
                    // For SKIP and COMMIT response codes, mark ack in ack
                    // manager and
                    // return
                    // DispatcherResponseAndOffset with COMMIT action.
                    return new DispatcherResponseAndOffset(
                        DispatcherResponse.Code.COMMIT, ackManager.ack(processorMessage));
                  case RETRY:
                  case RESQ:
                    // fallthrough
                  case DLQ:
                    // For RETRY and STASH actions, mark nack in ack manager.
                    // If nack returns false, we can skip producing to the retry
                    // topic or
                    // dlq topic.
                    // As for now, we always produce to the retry topic or dlq
                    // topic, will
                    // fix this
                    // in
                    // another change.
                    ackManager.nack(processorMessage.getPhysicalMetadata());
                    // Increase both attempt and  retry count.
                    processorMessage.increaseAttemptCount();
                    processorMessage.increaseRetryCount();
                    return new DispatcherResponseAndOffset(r.getCode(), -1);
                    // fallthrough: ack manager failed to nack, so we
                    // fallthrough to gRPC
                    // in-memory retry.
                  default:
                    // Increase only attempt count. This will allow the message
                    // to be
                    // retried in-memory
                    ackManager.nack(processorMessage.getPhysicalMetadata());
                    processorMessage.increaseAttemptCount();
                    return new DispatcherResponseAndOffset(DispatcherResponse.Code.INVALID, -1);
                }
              });
    }
  }

  /**
   * Timeout received - if maxTimeouts defined and there is DLQ, send message to DLQ - if message is
   * from resilience queue, in memory retry - if message is from DLQ, send message to DLQ
   *
   * @return a different response
   */
  @VisibleForTesting
  protected DispatcherResponse handleTimeout(
      DispatcherResponse dispatcherResponse, ProcessorMessage processorMessage, Job finalJob) {
    TopicPartitionOffset topicPartitionOffset = processorMessage.getPhysicalMetadata();
    String topic = topicPartitionOffset.getTopic();
    DispatcherResponse result = dispatcherResponse;
    switch (dispatcherResponse.getCode()) {
      case SKIP:
      case COMMIT:
        // credit token to DLQ limiter
        dlqDispatchManager.credit(
            new TopicPartition(
                topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition()),
            1);
        break;
      case BACKOFF:
        if (RetryUtils.isDLQTopic(topic, finalJob)) {
          // Messages in DLQ should not enter other queues
          result = new DispatcherResponse(DispatcherResponse.Code.DLQ);
        } else if (RetryUtils.isResqTopic(topic, finalJob)) {
          // messages in resilience queue should do in-memory retry to avoid leak to other queues
          result = new DispatcherResponse(DispatcherResponse.Code.INVALID);
        } else {
          // when maxRpcTimeouts is configured and timeout exceeds
          // limit
          // 1. acquire token from dlqDispatchManager
          // 2. convert BACKOFF to DLQ if token fetched
          int maxRpcTimeouts = finalJob.getRpcDispatcherTask().getMaxRpcTimeouts();
          if (maxRpcTimeouts > 0
              && processorMessage.getTimeoutCount() >= maxRpcTimeouts
              && dlqDispatchManager.tryAcquire(
                  new TopicPartition(
                      topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition()),
                  1)) {
            result = new DispatcherResponseAndOffset(DispatcherResponse.Code.DLQ);
          } else {
            // timeout as a case of failure in transit should be handled with in-memory retry and
            // Blocking detection/mitigation.
            // As current implementation already handle timeout with retry queue, change to
            // in-memory retry may cause blocking,
            // so temporary keep the behavior unchanged.
            result = new DispatcherResponseAndOffset(DispatcherResponse.Code.RETRY);
          }
        }
        // increase timeout count
        processorMessage.increaseTimeoutCount();
        break;
    }
    return result;
  }

  private Job getJob(long jobId, Job defaultJob) {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    // always get the job from pipelineStateManager, which has the latest configurations.
    // If the job is not an expected job any more, fall back to use the old job. That
    // means, even when a job is canceled, we still try to process and dispatch all
    // messages, because
    // 1. we don't want to silently skip any messages as it's not easy for the fetcher to
    // know whether messages are skipped or successfully processed.
    // 2. duplicated messages might not be a big problem for message receiving services.
    return pipelineStateManager.getExpectedJob(jobId).orElse(defaultJob);
  }

  /**
   * Creates a new Failsafe RetryPolicy with logging and metrics attached to all of the
   * PolicyListeners.
   *
   * @param scope to emit metrics on.
   * @param dispatcher for including in logs (since logger doesn't support subscoping).
   * @param rpcUri for including in logs (since logger doesn't support subscoping).
   * @param group for including in logs (since logger doesn't support subscoping).
   * @param topic for including in logs (since logger doesn't support subscoping).
   * @param partition for including in logs (since logger doesn't support subscoping).
   * @return RetryPolicy with logging and metrics.
   */
  private RetryPolicy<DispatcherResponseAndOffset> instrumentedRetryPolicy(
      Scope scope, String dispatcher, String rpcUri, String group, String topic, int partition) {
    return new RetryPolicy<DispatcherResponseAndOffset>()
        .onRetry(
            r -> {
              scope.counter(MetricNames.RETRYER_RETRY).inc(1);
              logRetryWarn(
                  MetricNames.RETRYER_RETRY,
                  dispatcher,
                  rpcUri,
                  group,
                  topic,
                  partition,
                  r.getLastFailure());
            })
        .onRetriesExceeded(
            r -> {
              scope.counter(MetricNames.RETRYER_RETRIES_EXCEEDED).inc(1);
              if (r.getFailure() == null) {
                LOGGER.error(
                    MetricNames.RETRYER_RETRIES_EXCEEDED,
                    StructuredLogging.dispatcher(dispatcher),
                    StructuredLogging.rpcRoutingKey(rpcUri),
                    StructuredLogging.kafkaGroup(group),
                    StructuredLogging.kafkaTopic(topic),
                    StructuredLogging.kafkaPartition(partition));
              } else {
                LOGGER.error(
                    MetricNames.RETRYER_RETRIES_EXCEEDED,
                    StructuredLogging.dispatcher(dispatcher),
                    StructuredLogging.rpcRoutingKey(rpcUri),
                    StructuredLogging.kafkaGroup(group),
                    StructuredLogging.kafkaTopic(topic),
                    StructuredLogging.kafkaPartition(partition),
                    r.getFailure());
              }
            })
        .onAbort(
            r -> {
              scope.counter(MetricNames.RETRYER_ABORT).inc(1);
              if (r.getFailure() == null) {
                LOGGER.warn(
                    MetricNames.RETRYER_ABORT,
                    StructuredLogging.dispatcher(dispatcher),
                    StructuredLogging.rpcRoutingKey(rpcUri),
                    StructuredLogging.kafkaGroup(group),
                    StructuredLogging.kafkaTopic(topic),
                    StructuredLogging.kafkaPartition(partition));
              } else {
                LOGGER.warn(
                    MetricNames.RETRYER_ABORT,
                    StructuredLogging.dispatcher(dispatcher),
                    StructuredLogging.rpcRoutingKey(rpcUri),
                    StructuredLogging.kafkaGroup(group),
                    StructuredLogging.kafkaTopic(topic),
                    StructuredLogging.kafkaPartition(partition),
                    r.getFailure());
              }
            })
        .onFailedAttempt(
            r -> {
              scope.counter(MetricNames.RETRYER_FAILED_ATTEMPT).inc(1);
              logRetryWarn(
                  MetricNames.RETRYER_FAILED_ATTEMPT,
                  dispatcher,
                  rpcUri,
                  group,
                  topic,
                  partition,
                  r.getLastFailure());
            });
  }

  private void logRetryWarn(
      String message,
      String dispatcher,
      String rpcUri,
      String group,
      String topic,
      int partition,
      Throwable t) {
    if (t != null) {
      LOGGER.warn(
          message,
          StructuredLogging.dispatcher(dispatcher),
          StructuredLogging.rpcRoutingKey(rpcUri),
          StructuredLogging.kafkaGroup(group),
          StructuredLogging.kafkaTopic(topic),
          StructuredLogging.kafkaPartition(partition),
          t);
    } else {
      LOGGER.warn(
          message,
          StructuredLogging.dispatcher(dispatcher),
          StructuredLogging.rpcRoutingKey(rpcUri),
          StructuredLogging.kafkaGroup(group),
          StructuredLogging.kafkaTopic(topic),
          StructuredLogging.kafkaPartition(partition));
    }
  }

  /**
   * Handles inflight limit permit status
   *
   * @param response
   * @param e
   * @param permit
   */
  @VisibleForTesting
  protected static void handlePermit(
      DispatcherResponse response, Throwable e, InflightLimiter.Permit permit) {
    // To measure downstream service availability, mark as unknown failure if
    // message timeout or with invalid response
    // mark as dropped if received specific back-pressure propagation signal
    // otherwise, mark permit succeed
    if (e != null) {
      // For any exception such as CancellationException close
      // permit with
      // unknown result
      // to avoid leak of count
      permit.complete();
    } else {
      switch (response.getCode()) {
        case INVALID: // unknown service error
        case BACKOFF: // timeout error
          permit.complete(InflightLimiter.Result.Failed);
          break;
        case DROPPED: // specific back-pressure
          permit.complete(InflightLimiter.Result.Dropped);
          break;
        default:
          permit.complete(InflightLimiter.Result.Succeed);
      }
    }
  }

  /**
   * return a stage that can be safely canceled without impact input stage
   *
   * @param stage
   * @return a stage
   */
  @VisibleForTesting
  protected static CompletionStage cancelSafeStage(CompletionStage stage) {
    return stage.whenComplete((r, t) -> {});
  }

  /**
   * Creates the Failsafe RetryPolicy for gRPC dispatcher.
   *
   * @param job to compute retry policy for.
   * @param scope to emit metrics on.
   * @return Failsafe RetryPolicy for gRPC dispatcher.
   */
  private RetryPolicy<DispatcherResponseAndOffset> getGrpcRetryPolicy(Job job, Scope scope) {
    final String dispatcher = DispatcherMessage.Type.GRPC.toString();
    final String rpcUri = job.getRpcDispatcherTask().getUri();
    final String group = job.getKafkaConsumerTask().getConsumerGroup();
    final String topic = job.getKafkaConsumerTask().getTopic();
    final int partition = job.getKafkaConsumerTask().getPartition();
    final Scope retryerScope =
        scope.tagged(ImmutableMap.of(StructuredLogging.DISPATCHER, dispatcher));
    return instrumentedRetryPolicy(retryerScope, dispatcher, rpcUri, group, topic, partition)
        .withMaxRetries(maxGrpcRetry)
        .abortIf(dispatcherResponseAndOffset -> !isRunning())
        // when the executor was shut down
        .abortOn(RejectedExecutionException.class)
        // exponential backoff from 1 milliseconds to 1 min
        // combined with Integer.MAX_VALUE retries means about 2 billion seconds = 63 years
        // until we exceed max retires, which is practically forever.
        .withBackoff(1, 60000, ChronoUnit.MILLIS)
        // By default all exceptions are handled.
        .handleResultIf(
            r -> {
              // do not retry when the processor is not running any more,
              if (!isRunning()) {
                return false;
              }
              switch (r.getCode()) {
                case SKIP:
                  // fallthrough to false
                case COMMIT:
                  // SKIP, COMMIT should not be retried to gRPC.
                  return proceedToRetry(
                      false, retryerScope, dispatcher, rpcUri, group, topic, partition);
                case DLQ:
                  // if dlq topic is configured, should not be retried to gRPC
                  if (!job.getRpcDispatcherTask().getDlqTopic().isEmpty()) {
                    return false;
                  }
                  // fallthrough to do next check
                case RETRY:
                  // if retry queue topic is configured, should not be retried to gRPC
                  if (RetryUtils.hasRetryTopic(job)) {
                    return false;
                  }
                  // No retry or DLQ retried to gRPC.
                  return proceedToRetry(
                      true, retryerScope, dispatcher, rpcUri, group, topic, partition);
                case RESQ:
                  // if resilience queue topic is configured, should not be retried to gRPC
                  if (RetryUtils.hasResqTopic(job)) {
                    return false;
                  }
                  // fallthrough to true
                default:
                  // All other response codes should be retried to gRPC.
                  return proceedToRetry(
                      true, retryerScope, dispatcher, rpcUri, group, topic, partition);
              }
            });
  }

  private boolean proceedToRetry(
      boolean result,
      Scope scope,
      String dispatcher,
      String routingKey,
      String group,
      String topic,
      int partition) {
    String resultName =
        result ? MetricNames.RETRYER_REJECT_RESULT : MetricNames.RETRYER_ACCEPT_RESULT;
    scope.counter(resultName).inc(1);
    LOGGER.debug(
        resultName,
        StructuredLogging.dispatcher(dispatcher),
        StructuredLogging.rpcRoutingKey(routingKey),
        StructuredLogging.kafkaGroup(group),
        StructuredLogging.kafkaTopic(topic),
        StructuredLogging.kafkaPartition(partition));
    return result;
  }

  /**
   * Creates a Fallback Failsafe Policy that sends to Kafka Retry or DLQ.
   *
   * @param record to dispatch to Kafka.
   * @param processingScope to emit metrics on.
   * @return AsyncFallback Failsafe Policy to be registered to Failsafe.
   */
  private Fallback<DispatcherResponseAndOffset> getKafkaProducerAsyncFallbackPolicy(
      ItemAndJob<ProcessorMessage> record, Scope processingScope) {
    final Scope dispatchingScope =
        processingScope.tagged(
            ImmutableMap.of(StructuredLogging.DISPATCHER, DispatcherMessage.Type.KAFKA.toString()));
    final ProcessorMessage processorMessage = record.getItem();
    final String dispatcher = DispatcherMessage.Type.KAFKA.toString();
    final Job job = record.getJob();
    final String rpcUri = job.getRpcDispatcherTask().getUri();
    final String group = job.getKafkaConsumerTask().getConsumerGroup();
    final String topic = job.getKafkaConsumerTask().getTopic();
    final int partition = job.getKafkaConsumerTask().getPartition();
    return Fallback.<DispatcherResponseAndOffset>ofStage(
            executionAttemptedEvent -> {
              DispatcherResponse.Code code = null;
              if (executionAttemptedEvent.getLastResult() == null) {
                Throwable lastFailure = executionAttemptedEvent.getLastFailure();
                if (lastFailure != null && lastFailure instanceof CancellationException) {
                  // retry policy canceled
                  code = processorMessage.getStub().cancelCode().get();
                }
              } else {
                code = executionAttemptedEvent.getLastResult().getCode();
              }

              // grpc dispatcher failed, propagate errors up.
              // TODO(T4772337): the fetcher needs to handle exceptions correctly.
              //  currently, the processor will eventually get stuck
              if (code == null) {
                CompletableFuture<DispatcherResponseAndOffset> completableFuture =
                    new CompletableFuture<>();
                completableFuture.completeExceptionally(executionAttemptedEvent.getLastFailure());
                return completableFuture;
              }

              // pick a topic to produce to based on code in DispatcherResponseAndOffset
              String topicToProduce = "";
              switch (code) {
                case DLQ:
                  topicToProduce = job.getRpcDispatcherTask().getDlqTopic();

                  if (!topicToProduce.isEmpty()) {
                    LOGGER.debug(
                        MetricNames.RETRYER_KAFKA_ACCEPT_RESULT,
                        StructuredLogging.dispatcher(dispatcher),
                        StructuredLogging.rpcRoutingKey(rpcUri),
                        StructuredLogging.kafkaGroup(group),
                        StructuredLogging.kafkaTopic(topic),
                        StructuredLogging.action(code.toString()),
                        StructuredLogging.kafkaPartition(partition));
                    break;
                  }
                  dispatchingScope.counter(MetricNames.MISSING_DLQ).inc(1);
                  // fallthrough to use retry topic
                case RETRY:

                  // If there's retry queue and enabled, find the one matches the retry count
                  // If the message has already exhausted all its retry count, then send to the DLQ,
                  // unless it's empty, sending to the last RQ
                  topicToProduce =
                      RetryUtils.getKafkaDestinationRetryTopic(
                          job, processorMessage.getRetryCount());

                  // The topicToProduce could be empty if:
                  //   1. There's a retry queue, in the new tiered retry format
                  //   2. There's no DLQ configured
                  //   3. Messages are returned from the consumer for a retriable error
                  // Since DLQ is the fallback queue of all the retry queues,
                  // getKafkaDestinationRetryTopic above will return DLQ, which is an empty string.
                  // To avoid proceeding with an empty queue, we will fallback to the default case,
                  // which is in-mem retry.
                  if (!topicToProduce.isEmpty()) {
                    LOGGER.debug(
                        MetricNames.RETRYER_KAFKA_ACCEPT_RESULT,
                        StructuredLogging.dispatcher(dispatcher),
                        StructuredLogging.rpcRoutingKey(rpcUri),
                        StructuredLogging.kafkaGroup(group),
                        StructuredLogging.kafkaTopic(topic),
                        StructuredLogging.action(code.toString()),
                        StructuredLogging.kafkaPartition(partition));
                    break;
                  }
                  dispatchingScope.counter(MetricNames.MISSING_DLQ_AND_RETRY_QUEUE).inc(1);
                  // This is not RETRY or STASH so do not send to Kafka producer and propagate
                  // errors
                  // up.
                  return CompletableFuture.completedFuture(executionAttemptedEvent.getLastResult());
                case RESQ:
                  if (RetryUtils.hasResqTopic(job)) {
                    topicToProduce = job.getResqConfig().getResqTopic();
                  }

                  if (!topicToProduce.isEmpty()) {
                    LOGGER.debug(
                        MetricNames.RETRYER_KAFKA_ACCEPT_RESULT,
                        StructuredLogging.dispatcher(dispatcher),
                        StructuredLogging.rpcRoutingKey(rpcUri),
                        StructuredLogging.kafkaGroup(group),
                        StructuredLogging.kafkaTopic(topic),
                        StructuredLogging.action(code.toString()),
                        StructuredLogging.kafkaPartition(partition));
                    break;
                  }
                  dispatchingScope.counter(MetricNames.MISSING_RESQ).inc(1);
                  // fallthrough to true
                default:
                  // There is no resilience queue, so do not send to Kafka producer and propagate
                  // errors up.
                  return CompletableFuture.completedFuture(executionAttemptedEvent.getLastResult());
              }
              // Else, we produce through to the broker.
              final Scope overallKafkaProducerScope =
                  processingScope.tagged(
                      ImmutableMap.of(StructuredLogging.DISPATCHER, MetricNames.OVERALL_KAFKA));
              final Stopwatch overallProduceLatency =
                  overallKafkaProducerScope.timer(MetricNames.DISPATCH_LATENCY).start();
              // Failsafe invokes policies in reverse. The callstack looks like:
              // 1. Kafka Producer
              // 2. Kafka Retry
              return Failsafe.with(getKafkaRetryPolicy(job, processingScope))
                  .with(Scheduler.of(executor))
                  .getStageAsync(getKafkaProducerSupplier(record, topicToProduce, processingScope))
                  .whenComplete(
                      instrumentDispatchCompletion(
                          overallKafkaProducerScope,
                          overallProduceLatency,
                          MetricNames.OVERALL_DISPATCHER,
                          rpcUri,
                          group,
                          topic,
                          partition));
            })
        .handleResultIf(
            result -> {
              // treat null as failure
              if (result == null) {
                return true;
              }
              switch (result.getCode()) {
                case RESQ:
                  // fallthrough
                case RETRY:
                  // fallthrough
                case DLQ:
                  // Treat RETRY and STASH as failures that should produce to KAFKA.
                  return true;
                  // TODO(T4772403): what should the fetcher do if we propagate the result up?
                  //  currently, the processor will eventually get stuck
                default:
                  return false;
              }
            });
  }

  private CheckedSupplier<CompletionStage<DispatcherResponseAndOffset>> getKafkaProducerSupplier(
      ItemAndJob<ProcessorMessage> record, String topicToProduce, Scope processingScope) {
    final Scope dispatchingScope =
        processingScope.tagged(
            ImmutableMap.of(
                StructuredLogging.DISPATCHER,
                DispatcherMessage.Type.KAFKA.toString(),
                StructuredLogging.DESTINATION,
                topicToProduce));
    final ProcessorMessage processorMessage = record.getItem();
    return () -> {
      final Job job = getJob(record.getJob().getJobId(), record.getJob());
      final String rpcUri = job.getRpcDispatcherTask().getUri();
      final String group = job.getKafkaConsumerTask().getConsumerGroup();
      final String topic = job.getKafkaConsumerTask().getTopic();
      final int partition = job.getKafkaConsumerTask().getPartition();
      Preconditions.checkNotNull(messageDispatcher, "message dispatcher required");
      dispatchingScope.counter(MetricNames.DISPATCH_CALLED).inc(1);
      LOGGER.debug(
          MetricNames.DISPATCH_CALLED,
          StructuredLogging.dispatcher(DispatcherMessage.Type.KAFKA.toString()),
          StructuredLogging.rpcRoutingKey(rpcUri),
          StructuredLogging.kafkaGroup(group),
          StructuredLogging.kafkaTopic(topic),
          StructuredLogging.kafkaPartition(partition));
      final Stopwatch dispatchLatency =
          dispatchingScope.timer(MetricNames.DISPATCH_LATENCY).start();

      return messageDispatcher
          .submit(ItemAndJob.of(processorMessage.getKafkaDispatcherMessage(topicToProduce), job))
          .thenApply(
              r -> {
                switch (r.getCode()) {
                  case SKIP:
                    // fallthrough
                  case COMMIT:
                    // SKIP and COMMIT response from Kafka Dispatcher represents successful produce
                    // so we can mark ack in ack manager.
                    return new DispatcherResponseAndOffset(
                        r.getCode(), ackManager.ack(processorMessage));
                  default:
                    return new DispatcherResponseAndOffset(r.getCode());
                }
              })
          .whenComplete(
              instrumentDispatchCompletion(
                  dispatchingScope,
                  dispatchLatency,
                  DispatcherMessage.Type.KAFKA.toString(),
                  rpcUri,
                  group,
                  topic,
                  partition));
    };
  }

  /**
   * Creates the Failsafe RetryPolicy for Kafka dispatcher.
   *
   * @param job to compute retry policy for.
   * @param scope to emit metrics on.
   * @return Failsafe RetryPolicy for Kafka dispatcher.
   */
  private RetryPolicy<DispatcherResponseAndOffset> getKafkaRetryPolicy(Job job, Scope scope) {
    final String dispatcher = DispatcherMessage.Type.KAFKA.toString();
    final String rpcUri = job.getRpcDispatcherTask().getUri();
    final String group = job.getKafkaConsumerTask().getConsumerGroup();
    final String topic = job.getKafkaConsumerTask().getTopic();
    final int partition = job.getKafkaConsumerTask().getPartition();
    final Scope retryerScope =
        scope.tagged(ImmutableMap.of(StructuredLogging.DISPATCHER, dispatcher));
    return instrumentedRetryPolicy(retryerScope, dispatcher, rpcUri, group, topic, partition)
        // Failsafe does not support unlimited retries.
        .withMaxRetries(maxKafkaRetry)
        .abortIf(dispatcherResponseAndOffset -> !isRunning())
        // when the executor was shut down
        .abortOn(RejectedExecutionException.class)
        // exponential backoff from 1 milliseconds to 1 min
        // combined with Integer.MAX_VALUE retries means about 2 billion seconds = 63 years
        // until we exceed max retires, which is practically forever.
        .withBackoff(1, 60000, ChronoUnit.MILLIS)
        // By default all exceptions are handled.
        .handleResultIf(
            r -> {
              // do not retry when the processor is not running any more,
              if (!isRunning()) {
                return false;
              }
              switch (r.getCode()) {
                case SKIP:
                  // fallthrough to false
                case COMMIT:
                  // SKIP, COMMIT is considered success for Kafka producing
                  retryerScope.counter(MetricNames.RETRYER_ACCEPT_RESULT).inc(1);
                  LOGGER.debug(
                      MetricNames.RETRYER_ACCEPT_RESULT,
                      StructuredLogging.dispatcher(dispatcher),
                      StructuredLogging.rpcRoutingKey(rpcUri),
                      StructuredLogging.kafkaGroup(group),
                      StructuredLogging.kafkaTopic(topic),
                      StructuredLogging.kafkaPartition(partition));
                  return false;
                default:
                  // All other response codes should be retried to Kafka.
                  retryerScope.counter(MetricNames.RETRYER_REJECT_RESULT).inc(1);
                  LOGGER.warn(
                      MetricNames.RETRYER_REJECT_RESULT,
                      StructuredLogging.dispatcher(dispatcher),
                      StructuredLogging.rpcRoutingKey(rpcUri),
                      StructuredLogging.kafkaGroup(group),
                      StructuredLogging.kafkaTopic(topic),
                      StructuredLogging.kafkaPartition(partition));
                  return true;
              }
            });
  }

  /**
   * Submits a consumerRecord and the associated job definition; returns a CompletionStage which,
   * when it completes, returns an offset to commit to the Kafka server side Notice that this method
   * blocks when the processor cannot take more consumerRecords
   *
   * @param request the consumerRecord to process
   * @return a CompletionStage which, when it completes, returns an offset to commit to the Kafka
   *     server side
   */
  @Override
  public CompletionStage<Long> submit(ItemAndJob<ConsumerRecord<byte[], byte[]>> request) {
    final Job job = request.getJob();
    final ConsumerRecord<byte[], byte[]> consumerRecord = request.getItem();
    final StructuredArgument[] logTags =
        new StructuredArgument[] {
          StructuredArguments.keyValue(StructuredFields.URI, addressFromUri),
          StructuredArguments.keyValue(
              StructuredFields.KAFKA_GROUP, job.getKafkaConsumerTask().getConsumerGroup()),
          StructuredArguments.keyValue(
              StructuredFields.KAFKA_CLUSTER, job.getKafkaConsumerTask().getCluster()),
          StructuredArguments.keyValue(
              StructuredFields.KAFKA_TOPIC, job.getKafkaConsumerTask().getTopic()),
          StructuredArguments.keyValue(
              StructuredFields.KAFKA_PARTITION,
              Integer.toString(job.getKafkaConsumerTask().getPartition()))
        };
    Map<String, String> metricsTags = getMetricsTags(job);
    final String[] tags =
        metricsTags
            .entrySet()
            .stream()
            .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
            .toArray(String[]::new);

    infra.contextManager().createRootContext();

    // TODO (gteo): cleanup / deprecate processing scope
    Scope processingScope = infra.scope().tagged(metricsTags);

    // use completable future chaining so that any exception thrown by an earlier stage
    // will automatically terminate the chain.
    final Stopwatch prefetchLatency =
        processingScope.timer("processor.prefetch.overall.latency").start();
    return Instrumentation.instrument
        .withExceptionalCompletion(
            LOGGER,
            infra.scope(),
            // Create a new processor message
            // DirectSupplier creates it on this thread.
            () ->
                DirectSupplier.supply(
                    () -> {
                      ProcessorMessage processorMessage =
                          ProcessorMessage.of(
                              consumerRecord,
                              job,
                              infra,
                              new MessageStub(logTags, processingScope));
                      acquireQuota(job, processorMessage);
                      return processorMessage;
                    }),
            "processor.prefetch.new-message",
            tags)
        .thenApply(
            pm ->
                Instrumentation.instrument.withRuntimeException(
                    LOGGER,
                    infra.scope(),
                    // Enqueue this to the ack manager, which acts as a prefetch buffer and
                    // out of order commit tracker.
                    () -> {
                      ackManager.receive(pm);
                      return pm;
                    },
                    "processor.prefetch.ack-manager.receive",
                    tags))
        .thenApply(
            pm ->
                Instrumentation.instrument.withRuntimeException(
                    LOGGER,
                    infra.scope(),
                    () -> {
                      if (!messageFilter.shouldProcess(ItemAndJob.of(pm, job))) {
                        pm.setOffsetToCommit(ackManager.ack(pm));
                        pm.setShouldDispatch(false);

                        processingScope
                            .counter("processor.prefetch.filter.cluster.filtered")
                            .inc(1);
                        LOGGER.debug("processor.prefetch.filter.cluster.filtered", logTags);
                      }
                      return pm;
                    },
                    "processor.prefetch.filter.cluster",
                    tags))
        .thenApplyAsync(
            // From here onwards, we enter the dispatch phase, where we send messages via gRPC or to
            // DLQ.
            // The dispatch phases uses separate executor pool so that we can send multiple messages
            // concurrently.
            pm -> pm,
            executor)
        .whenComplete(
            (r, t) -> {
              // metrics and log overall prefetch stage.
              prefetchLatency.stop();
              if (t != null) {
                processingScope
                    .tagged(ImmutableMap.of(Tags.Key.code, t.getClass().getSimpleName()))
                    .counter("processor.prefetch.overall")
                    .inc(1);
              } else {
                processingScope
                    .tagged(ImmutableMap.of(Tags.Key.code, "ok"))
                    .counter("processor.prefetch.overall")
                    .inc(1);
              }
            })
        .thenCompose(
            pm ->
                infra
                    .contextManager()
                    .runAsync(infra.contextManager()::createRootContext, executor)
                    .thenApply(ignoredVoid -> pm))
        .thenCompose(
            pm ->
                supplyAsyncIfDispatchable(
                    pm,
                    () ->
                        Instrumentation.instrument.withExceptionalCompletion(
                            LOGGER,
                            infra.scope(),
                            () -> {
                              // Forward the messages to the dispatchers
                              // This handles the actual transmission to callee via gRPC
                              // and any follow up transmission to Kafka for DLQ.
                              return Failsafe.with(
                                      getKafkaProducerAsyncFallbackPolicy(
                                          ItemAndJob.of(pm, job), processingScope))
                                  .with(Scheduler.of(executor))
                                  .getStageAsync(
                                      () ->
                                          pm.getStub()
                                              .withRetryFuture(
                                                  Failsafe.with(
                                                          getGrpcRetryPolicy(job, processingScope))
                                                      .with(Scheduler.of(executor))
                                                      .getStageAsync(
                                                          infra
                                                              .contextManager()
                                                              .wrap(
                                                                  dispatchToGrpcOutbound(
                                                                      ItemAndJob.of(pm, job),
                                                                      tags)))))
                                  .whenComplete(
                                      (r, t) -> {
                                        try {
                                          pm.close(r, t);
                                        } catch (Exception e) {
                                          LOGGER.error("failed to close processor message", e);
                                        }
                                      })
                                  .thenApply(
                                      r -> {
                                        pm.setOffsetToCommit(r.getOffset());
                                        return pm;
                                      });
                            },
                            "processor.dispatch",
                            tags)))
        .thenApply(
            // Convert response to commit offsets.
            r -> r.getOffsetToCommit());
  }

  /** Invokes the supplier if ProcessorMessage.shouldDispatch() returns true. */
  private static ProcessorMessage supplyIfDispatchable(
      ProcessorMessage pm, Supplier<ProcessorMessage> supplier) {
    if (pm.shouldDispatch()) {
      return supplier.get();
    }
    return pm;
  }

  /** Invokes the supplier if ProcessorMessage.shouldDispatch() returns true. */
  private static CompletionStage<ProcessorMessage> supplyAsyncIfDispatchable(
      ProcessorMessage pm, Supplier<CompletionStage<ProcessorMessage>> supplier) {
    if (pm.shouldDispatch()) {
      return supplier.get();
    }
    return CompletableFuture.completedFuture(pm);
  }

  /**
   * Instruments dispatch completion.
   *
   * @param scope to emit metrics on.
   * @param latencyStopwatch to stop upon completion.
   * @param dispatcher name to include in logs (since logger does not support subscoping).
   * @param rpcUri name to include in logs (since logger does not support subscoping).
   * @param group name to include in logs (since logger does not support subscoping).
   * @param topic name to include in logs (since logger does not support subscoping).
   * @param partition name to include in logs (since logger does not support subscoping).
   * @return BiConsumer that can be attached to CompletionStage via whenComplete.
   */
  private static <R, T extends Throwable> BiConsumer<R, T> instrumentDispatchCompletion(
      Scope scope,
      Stopwatch latencyStopwatch,
      String dispatcher,
      String rpcUri,
      String group,
      String topic,
      int partition) {
    return (r, t) -> {
      latencyStopwatch.stop();
      if (t != null) {
        String error = t.getMessage();
        scope
            .tagged(ImmutableMap.of(StructuredLogging.REASON, error != null ? error : "unknown"))
            .counter(MetricNames.DISPATCH_FAILED)
            .inc(1);
        LOGGER.error(
            MetricNames.DISPATCH_FAILED,
            StructuredLogging.dispatcher(dispatcher),
            StructuredLogging.rpcRoutingKey(rpcUri),
            StructuredLogging.kafkaGroup(group),
            StructuredLogging.kafkaTopic(topic),
            StructuredLogging.kafkaPartition(partition),
            t);
      } else {
        scope.counter(MetricNames.DISPATCH_SUCCESS).inc(1);
        LOGGER.debug(
            MetricNames.DISPATCH_SUCCESS,
            StructuredLogging.dispatcher(dispatcher),
            StructuredLogging.rpcRoutingKey(rpcUri),
            StructuredLogging.kafkaGroup(group),
            StructuredLogging.kafkaTopic(topic),
            StructuredLogging.kafkaPartition(partition));
      }
    };
  }

  @Override
  public void setNextStage(Sink<DispatcherMessage, DispatcherResponse> messageDispatcher) {
    this.messageDispatcher = messageDispatcher;
  }

  @Override
  public void setPipelineStateManager(PipelineStateManager pipelineStateManager) {
    this.pipelineStateManager = pipelineStateManager;
  }

  @Override
  public void start() {
    LOGGER.info("starting message processor");
    isRunning.set(true);
    LOGGER.info("started message processor");
  }

  @Override
  public boolean isRunning() {
    return isRunning.get();
  }

  @Override
  public void stop() {
    LOGGER.info("stopping message processor");
    try {
      // clean up
      cancelAll().toCompletableFuture().get();
    } catch (Exception e) {
      LOGGER.error("failed to stop message processor", e);
      infra.scope().counter(MetricNames.CLOSE_FAILURE).inc(1);
      throw new RuntimeException(e);
    }
    // shutdown executors
    executor.shutdown();
    outboundMessageLimiter.close();
    isRunning.set(false);
    infra.scope().counter(MetricNames.CLOSE_SUCCESS).inc(1);
    LOGGER.info("stopped message processor");
  }

  private static void logCommand(String command, Job job) {
    LOGGER.info(
        "{} on processor",
        command,
        com.uber.data.kafka.datatransfer.common.StructuredLogging.jobId(job.getJobId()),
        com.uber.data.kafka.datatransfer.common.StructuredLogging.kafkaTopic(
            job.getKafkaConsumerTask().getTopic()),
        com.uber.data.kafka.datatransfer.common.StructuredLogging.kafkaPartition(
            job.getKafkaConsumerTask().getPartition()),
        com.uber.data.kafka.datatransfer.common.StructuredLogging.kafkaCluster(
            job.getKafkaConsumerTask().getCluster()),
        com.uber.data.kafka.datatransfer.common.StructuredLogging.kafkaGroup(
            job.getKafkaConsumerTask().getConsumerGroup()));
  }

  @Override
  public CompletionStage<Void> run(Job job) {
    return CompletableFuture.runAsync(
        () -> {
          outboundMessageLimiter.init(job);
          dlqDispatchManager.init(job);
          ackManager.init(job);
          updateQuota();
          logCommand(CommandType.COMMAND_TYPE_RUN_JOB.toString(), job);
        });
  }

  @Override
  public CompletionStage<Void> update(Job job) {
    return CompletableFuture.runAsync(
        () -> {
          updateQuota();
          logCommand(CommandType.COMMAND_TYPE_UPDATE_JOB.toString(), job);
        });
  }

  @Override
  public CompletionStage<Void> cancel(Job job) {
    return CompletableFuture.runAsync(
        () -> {
          updateQuota();
          ackManager.cancel(job);
          dlqDispatchManager.cancel(job);
          outboundMessageLimiter.cancel(job);
          logCommand(CommandType.COMMAND_TYPE_CANCEL_JOB.toString(), job);
        });
  }

  @Override
  public CompletionStage<Void> cancelAll() {
    return CompletableFuture.runAsync(
        () -> {
          updateQuota();
          ackManager.cancelAll();
          dlqDispatchManager.cancelAll();
          outboundMessageLimiter.cancelAll();
          LOGGER.info("cancelall on processor");
        });
  }

  private void updateQuota() {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    messageRateLimiter.setRate(pipelineStateManager.getFlowControl().getMessagesPerSec());
    byteRateLimiter.setRate(pipelineStateManager.getFlowControl().getBytesPerSec());
    outboundMessageLimiter.updateLimit(
        (int)
            Math.round(Math.ceil(pipelineStateManager.getFlowControl().getMaxInflightMessages())));
  }

  private void acquireQuota(Job job, ProcessorMessage processorMessage) {
    messageRateLimiter.acquire();
    if (processorMessage.getValueByteSize() > 0) {
      byteRateLimiter.acquire(processorMessage.getValueByteSize());
    } else {
      LOGGER.error(
          "received empty message",
          StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
          StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
          StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()),
          StructuredLogging.kafkaOffset(processorMessage.getPhysicalMetadata().getOffset()));
    }
  }

  @Override
  public void publishMetrics() {
    ackManager.publishMetrics();
    outboundMessageLimiter.publishMetrics();
    dlqDispatchManager.publishMetrics();
  }

  @VisibleForTesting
  protected Map<String, String> getMetricsTags(Job job) {
    ImmutableMap.Builder<String, String> tagsBuilder = ImmutableMap.builder();
    tagsBuilder.put(StructuredFields.URI, addressFromUri);
    tagsBuilder.put(StructuredFields.KAFKA_GROUP, job.getKafkaConsumerTask().getConsumerGroup());
    tagsBuilder.put(StructuredFields.KAFKA_CLUSTER, job.getKafkaConsumerTask().getCluster());
    tagsBuilder.put(StructuredFields.KAFKA_TOPIC, job.getKafkaConsumerTask().getTopic());
    tagsBuilder.put(
        StructuredFields.KAFKA_PARTITION,
        Integer.toString(job.getKafkaConsumerTask().getPartition()));
    if (job.hasMiscConfig()) {
      tagsBuilder.put(
          StructuredFields.CONSUMER_SERVICE_NAME, job.getMiscConfig().getOwnerServiceName());
    }
    return tagsBuilder.build();
  }

  @VisibleForTesting
  protected OutboundMessageLimiter getOutboundMessageLimiter() {
    return outboundMessageLimiter;
  }

  private static class MetricNames {
    // For dispatcher metrics, we use the same "processor.dispatch.latency" as the metric name.
    // And we add a tag dispatcher = overall | grpc | kafka to distinguish which dispatcher we are
    // targetting.
    // This allows us to aggregate on latency across modes.
    static final String DISPATCH_CALLED = "dispatch.called";
    static final String DISPATCH_SUCCESS = "dispatch.success";
    static final String DISPATCH_FAILED = "dispatch.failed";
    static final String DISPATCH_LATENCY = "dispatch.latency";
    static final String OVERALL_DISPATCHER = "overall";
    // TODO(T4765209): add metric report for overall grpc
    static final String OVERALL_GRPC = "overall-grpc";
    static final String OVERALL_KAFKA = "overall-kafka";

    // For Failsafe retry policy metrics, we emit standardized metrics with mode = grpc | kafka
    // to allow easy aggregation across types for a single job.
    // The following are emitted by the logic that chooses to accept or reject (retry) a result.
    static final String RETRYER_ACCEPT_RESULT = "processor.retryer.result.accept";
    static final String RETRYER_KAFKA_ACCEPT_RESULT = "processor.retryer.result.kafka.accept";
    static final String RETRYER_REJECT_RESULT = "processor.retryer.result.reject";
    // The following are emitted in the Failsafe retry listeners.
    static final String RETRYER_RETRY = "processor.retryer.retry";
    static final String RETRYER_RETRIES_EXCEEDED = "processor.retryer.retriesExceeded";
    static final String RETRYER_ABORT = "processor.retryer.abort";
    static final String RETRYER_FAILED_ATTEMPT = "processor.retryer.failedAttempt";

    static final String CLOSE_SUCCESS = "processor.close.success";
    static final String CLOSE_FAILURE = "processor.close.failure";
    static final String BYTES_RATE = "processor.dispatch.grpc.bytes";

    static final String MISSING_DLQ = "processor.dispatch.kafka.missing.dlq";
    static final String MISSING_DLQ_AND_RETRY_QUEUE =
        "processor.dispatch.kafka.missing.dlq.and.retry.queue";
    static final String MISSING_RESQ = "processor.dispatch.kafka.missing.resq";

    static final String RESPONSE_DISTRIBUTION = "response.distribution";

    static final String MESSAGE_END_TO_END_LATENCY = "message.e2e.latency";
  }
}
