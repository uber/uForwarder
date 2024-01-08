package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.data.kafka.consumerproxy.utils.RetryUtils;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.RoutingUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.instrumentation.Tags;
import com.uber.m3.tally.Scope;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BlockingQueueStubManager} detects and cancels blocking message
 *
 * <pre>
 *   1. when received a message, it first puts the message into {@link StubManager} and generates corresponding {@link MessageStub}.
 *      attaches the generated {@link MessageStub} to {@link ProcessorMessage}, for {@link ProcessorImpl} to add cancellation reactor later
 *   2. afterwards, enumerate {@link BlockingQueue} to detect blocking message
 *   3. if any blocking message detected, enumerates rules to resolve blocking message, and if any rule matched, translate match result
 *      to {@link DispatcherResponse.Code} and mark the blocking message in all {@link BlockingQueue} as canceled
 *   4. finally invoke {@link MessageStub}.cancel() to notify reactors to perform cancellation
 *   5. canceled messages should finally removed from all {@link BlockingQueue}
 * </pre>
 *
 * <pre>
 *   Token limiting is introduced to make sure cancellation works only if consumer is still active.
 *   It grants token when a message removed from the manager
 *   and acquires token to initiate cancellation
 * </pre>
 */
public class BlockingQueueStubManager extends StubManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(BlockingQueueStubManager.class);
  // One head of line blocking resolution consumes token generated from 50 succeed ack, to
  // tolerate 2% message failure
  private static final int COST_TOKEN_RETRY = 50;
  // With resilience queue, one resolution of blocking message cost 2 tokens from succeed ack
  // to tolerate 50% message failure
  private static final int COST_TOKEN_RESQ = 2;
  private static final BiConsumer<BlockingQueue.BlockingReason, CancelResult> NOOP_LISTENER =
      (k, v) -> {};
  /**
   * rules are evaluated in ascending order
   *
   * <pre>
   * if blocked by poison pill message and there is DLQ, return STASH
   * if it's head of line blocking and there is Retry queue, return RETRY
   * </pre>
   */
  private static final Rule[] RULES =
      new Rule[] {
        new Rule(
            new BlockingQueue.BlockingReason[] {
              BlockingQueue.BlockingReason.BLOCKING, BlockingQueue.BlockingReason.SATURATION
            },
            // message in resilience queue can't be mitigated with resilience queue
            job ->
                !RetryUtils.isResqTopic(job.getKafkaConsumerTask().getTopic(), job)
                    && RetryUtils.hasResqTopic(job),
            COST_TOKEN_RESQ,
            DispatcherResponse.Code.RESQ),
        new Rule(
            new BlockingQueue.BlockingReason[] {BlockingQueue.BlockingReason.BLOCKING},
            // message in retry queue or resilience queue can't be mitigated with retry queue
            job ->
                !RetryUtils.isRetryTopic(job.getKafkaConsumerTask().getTopic(), job)
                    && !RetryUtils.isResqTopic(job.getKafkaConsumerTask().getTopic(), job)
                    && RetryUtils.hasRetryTopic(job),
            COST_TOKEN_RETRY,
            DispatcherResponse.Code.RETRY)
      };

  private final Map<TopicPartition, BlockingResolver> tpResolver;
  private final Set<BlockingQueue> blockingQueues;
  private final Scope scope;
  private final TokenLimiter tokenLimiter;
  private BiConsumer<BlockingQueue.BlockingReason, CancelResult> cancelListener;

  /**
   * Instantiates a new Blocking queue stub manager. In dryRun mode, cancel doesn't actual impact
   * message processing
   *
   * @param scope the scope
   */
  BlockingQueueStubManager(Scope scope) {
    super(scope);
    this.tpResolver = new ConcurrentHashMap<>();
    this.blockingQueues = new LinkedHashSet<>();
    this.tokenLimiter = WindowedTokenLimiter.newBuilder().build();
    this.scope = scope;
    this.cancelListener = NOOP_LISTENER;
  }

  @VisibleForTesting
  protected void setCancelListener(
      BiConsumer<BlockingQueue.BlockingReason, CancelResult> cancelListener) {
    this.cancelListener = cancelListener;
  }

  void addBlockingQueue(BlockingQueue blockingQueue) {
    blockingQueues.add(blockingQueue);
  }

  /**
   * Cancels the topic partition
   *
   * @param topicPartition the topic partition
   */
  @Override
  void cancel(TopicPartition topicPartition) {
    tpResolver.remove(topicPartition);
    super.cancel(topicPartition);
  }

  /** Cancels all topic partition */
  @Override
  void cancelAll() {
    tpResolver.clear();
    super.cancelAll();
  }

  /**
   * Subscribes the topic partition
   *
   * @param job the job
   */
  @Override
  void init(Job job) {
    super.init(job);
    TopicPartition topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    tpResolver.put(topicPartition, new BlockingResolver(job, topicPartition));
  }

  /**
   *
   *
   * <pre>
   *  1. generate message stub
   *  2. detects blocking message
   *  3. resolve blocking message by invoke stub.cancel
   * </pre>
   *
   * @param pm the ProcessorMessage
   * @return if remove succeed
   */
  @Override
  boolean ack(ProcessorMessage pm) {
    if (!super.ack(pm)) {
      return false;
    }

    tokenLimiter.credit(1);
    TopicPartitionOffset topicPartitionOffset = pm.getPhysicalMetadata();
    TopicPartition topicPartition =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    tryResolveBlocking(topicPartition);

    return true;
  }

  @Override
  void nack(TopicPartitionOffset topicPartitionOffset) {
    TopicPartition topicPartition =
        new TopicPartition(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    tryResolveBlocking(topicPartition);
  }

  @VisibleForTesting
  protected void addTokens(int tokens) {
    tokenLimiter.credit(tokens);
  }

  @VisibleForTesting
  protected int getTokens() {
    return tokenLimiter.getMetrics().getNumTokens();
  }

  private void tryResolveBlocking(TopicPartition topicPartition) {
    BlockingResolver resolver = tpResolver.get(topicPartition);
    if (resolver == null) {
      logAndReportUnassignedTopicPartition(topicPartition);
    } else {
      Optional<CancelMessage> cancelMessage = resolver.tryResolve();
      if (cancelMessage.isPresent()) {
        Optional<MessageStub> stub = getStub(cancelMessage.get().metadata);
        if (stub.isPresent()) {
          DispatcherResponse.Code code = cancelMessage.get().result.responseCode;
          cancelMessage.get().result.close(stub.get().cancel(code));
        }
      }
    }
  }

  private class BlockingResolver {
    private final Job job;
    private final TopicPartition topicPartition;
    private final Optional<Rule> optRule;

    private BlockingResolver(Job job, TopicPartition topicPartition) {
      this.job = job;
      this.topicPartition = topicPartition;
      Rule jobRule = null;
      if (!RetryUtils.isDLQTopic(job.getKafkaConsumerTask().getTopic(), job)) {
        // messages in DLQ are not retriable thus should not support cancel
        for (Rule rule : RULES) {
          // find first matching rule
          if (rule.matchingJob.test(job)) {
            jobRule = rule;
            break;
          }
        }
      }

      optRule = Optional.ofNullable(jobRule);
    }

    /** detects and mark blocking messages as canceled the method is threadsafe */
    private synchronized Optional<CancelMessage> tryResolve() {
      // detects blocking messages
      BlockingQueue.BlockingMessage blockingMessage = null;
      for (BlockingQueue blockingQueue : blockingQueues) {
        Optional<BlockingQueue.BlockingMessage> optionalBlockingMessage =
            blockingQueue.detectBlockingMessage(topicPartition);
        if (optionalBlockingMessage.isPresent()) {
          blockingMessage = optionalBlockingMessage.get();
          break;
        }
      }

      // mark blocking messages as canceled
      if (blockingMessage != null) {
        TopicPartitionOffset cancelMetadata = blockingMessage.getMetaData();
        BlockingQueue.BlockingReason reason = blockingMessage.getReason();
        CancelResult result = runCancelRules(reason);
        logAndReportCancelResult(job, reason, result);

        if (result.isSucceed()) {
          for (BlockingQueue blockingQueue : blockingQueues) {
            blockingQueue.markCanceled(cancelMetadata);
          }
          return Optional.of(new CancelMessage(result, cancelMetadata));
        }
      }

      return Optional.empty();
    }

    /**
     * Gets corresponding response code if matched any cancellation rule. otherwise returns an error
     * result
     *
     * @param reason
     * @return
     */
    private CancelResult runCancelRules(BlockingQueue.BlockingReason reason) {
      if (!optRule.isPresent()) {
        return new CancelResult(CancelResult.ErrorCode.JOB_NOT_SUPPORTED);
      }

      Rule rule = optRule.get();

      if (!rule.matchingReasons.contains(reason)) {
        return new CancelResult(CancelResult.ErrorCode.REASON_NOT_MATCH);
      }

      if (!tokenLimiter.tryAcquire(rule.costToken)) {
        return new CancelResult(CancelResult.ErrorCode.RATE_LIMITED);
      }

      return new TokenCancelResult(rule.responseCode, rule.costToken);
    }

    private void logAndReportCancelResult(
        Job job, BlockingQueue.BlockingReason reason, CancelResult result) {
      cancelListener.accept(reason, result);
      Map jobTags =
          ImmutableMap.of(
              StructuredFields.URI,
              RoutingUtils.extractAddress(job.getRpcDispatcherTask().getUri()),
              StructuredFields.KAFKA_GROUP,
              job.getKafkaConsumerTask().getConsumerGroup(),
              StructuredFields.KAFKA_CLUSTER,
              job.getKafkaConsumerTask().getCluster(),
              StructuredFields.KAFKA_TOPIC,
              job.getKafkaConsumerTask().getTopic(),
              StructuredFields.KAFKA_PARTITION,
              Integer.toString(job.getKafkaConsumerTask().getPartition()));
      scope
          .tagged(jobTags)
          .tagged(
              ImmutableMap.of(
                  Tags.Key.result,
                  result.isSucceed() ? result.responseCode.name() : result.errorCode.name()))
          .counter(MetricNames.CANCEL_RESULT)
          .inc(1);
      scope
          .tagged(jobTags)
          .tagged(ImmutableMap.of(Tags.Key.reason, reason.name()))
          .counter(MetricNames.CANCEL_REASON)
          .inc(1);
    }
  }

  class TokenCancelResult extends CancelResult {
    private final int token;

    TokenCancelResult(DispatcherResponse.Code responseCode, int token) {
      super(responseCode);
      this.token = token;
    }

    @Override
    boolean close(boolean succeed) {
      if (super.close(succeed)) {
        if (!succeed) {
          // if cancel failed, return credit
          tokenLimiter.credit(token);
        }
        return true;
      }
      return false;
    }
  }

  private static class Rule {
    // state to machine to apply the rule
    private final Set<BlockingQueue.BlockingReason> matchingReasons;
    // condition job should match to apply the rule
    private final Predicate<Job> matchingJob;
    // number of tokens needed to apply the rule
    private final int costToken;
    // result of the rule
    private final DispatcherResponse.Code responseCode;

    Rule(
        BlockingQueue.BlockingReason[] reasons,
        Predicate<Job> matchingJob,
        int costToken,
        DispatcherResponse.Code responseCode) {
      this.matchingReasons = ImmutableSet.copyOf(reasons);
      this.matchingJob = matchingJob;
      this.costToken = costToken;
      this.responseCode = responseCode;
    }
  }

  private static class CancelMessage {
    private final CancelResult result;
    private final TopicPartitionOffset metadata;

    CancelMessage(CancelResult result, TopicPartitionOffset metadata) {
      this.result = result;
      this.metadata = metadata;
    }
  }

  private static class MetricNames {
    static final String CANCEL_RESULT = "tracking-queue.blocking.cancel-result";
    static final String CANCEL_REASON = "tracking-queue.blocking.cancel-reason";
  }
}
