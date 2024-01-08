package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.common.MetricsUtils;
import com.uber.data.kafka.consumerproxy.utils.RetryUtils;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import com.uber.m3.tally.Scope;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.TopicPartition;

/** DlqDispatchManager grant permits to dispatch messages to DLQ. */
public class DlqDispatchManager implements MetricSource {
  private static final long DLQ_TOKEN_LIMITER_WINDOW_MILLIS =
      TimeUnit.MINUTES.toMillis(5); // tokens expires after 5 minutes
  private static final int DLQ_TOKEN_LIMITER_DEFAULT_TOKENS =
      1; // number of free token in every window duration, make it positive to avoid consumer stuck
  private final ConcurrentMap<TopicPartition, ScopeAndTokenLimiter> tpTokenLimiters;
  private final Scope scope;

  DlqDispatchManager(Scope scope) {
    this.scope = scope;
    this.tpTokenLimiters = new ConcurrentHashMap<>();
  }

  /**
   * Credit number of tokens to DLQ dispatching
   *
   * @param tp the topic partition
   * @param n the number of tokens to credit
   */
  void credit(TopicPartition tp, int n) {
    ScopeAndTokenLimiter scopeAndTokenLimiter = tpTokenLimiters.get(tp);
    if (scopeAndTokenLimiter != null) {
      scopeAndTokenLimiter.tokenLimiter.credit(n);
      scopeAndTokenLimiter.scope.counter(MetricNames.DLQ_TOKEN_LIMITER_CREDIT).inc(n);
    }
  }

  /**
   * Acquire number of tokens to DLQ dispatching
   *
   * @param tp the topic partition
   * @param n the number of tokens to acquire
   */
  boolean tryAcquire(TopicPartition tp, int n) {
    ScopeAndTokenLimiter scopeAndTokenLimiter = tpTokenLimiters.get(tp);
    if (scopeAndTokenLimiter == null) {
      return true;
    }
    boolean approved = scopeAndTokenLimiter.tokenLimiter.tryAcquire(n);
    scopeAndTokenLimiter
        .scope
        .counter(
            approved
                ? MetricNames.DLQ_TOKEN_LIMITER_APPROVAL
                : MetricNames.DLQ_TOKEN_LIMITER_REJECT)
        .inc(1);
    return approved;
  }

  /**
   * Gets the number of available tokens
   *
   * @param tp the topic partition
   * @return the number of available tokens
   */
  int getTokens(TopicPartition tp) {
    ScopeAndTokenLimiter scopeAndTokenLimiter = tpTokenLimiters.get(tp);
    if (scopeAndTokenLimiter == null) {
      return 0;
    }

    return scopeAndTokenLimiter.tokenLimiter.getMetrics().getNumTokens();
  }

  void init(Job job) {
    tpTokenLimiters.computeIfAbsent(
        toTopicPartition(job), tp -> new ScopeAndTokenLimiter(job, scope));
  }

  void cancel(Job job) {
    tpTokenLimiters.remove(toTopicPartition(job));
  }

  void cancelAll() {
    tpTokenLimiters.clear();
  }

  @Override
  public void publishMetrics() {
    tpTokenLimiters.forEach((tp, scopeAndLimiter) -> scopeAndLimiter.publishMetrics());
  }

  private static TopicPartition toTopicPartition(Job job) {
    return new TopicPartition(
        job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
  }

  private class ScopeAndTokenLimiter implements MetricSource {
    private final Scope scope;
    private final TokenLimiter tokenLimiter;

    ScopeAndTokenLimiter(Job job, Scope scope) {
      this.scope = MetricsUtils.jobScope(scope, job);

      // Do not limit stash when source topic is a dlq topic
      tokenLimiter =
          RetryUtils.isDLQTopic(job.getKafkaConsumerTask().getTopic(), job)
              ? TokenLimiter.NOOP
              : WindowedTokenLimiter.newBuilder()
                  .withWindowMillis(DLQ_TOKEN_LIMITER_WINDOW_MILLIS)
                  .withDefaultTokens(DLQ_TOKEN_LIMITER_DEFAULT_TOKENS)
                  .build();
    }

    @Override
    public void publishMetrics() {
      scope
          .gauge(MetricNames.DLQ_TOKEN_LIMITER_TOKENS)
          .update(tokenLimiter.getMetrics().getNumTokens());
    }
  }

  private static class MetricNames {
    static final String DLQ_TOKEN_LIMITER_APPROVAL = "dlq.token.limiter.approval";
    static final String DLQ_TOKEN_LIMITER_CREDIT = "dlq.token.limiter.credit";
    static final String DLQ_TOKEN_LIMITER_REJECT = "dlq.token.limiter.reject";
    static final String DLQ_TOKEN_LIMITER_TOKENS = "dlq.token.limiter.tokens";
  }
}
