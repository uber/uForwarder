package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;

/** PipelineHealthManager tracks and reports health issue of a pipeline. */
public class PipelineHealthManager implements MetricSource {
  // heath state window duration
  // TODO make this configurable
  private static final Duration stateWindowDuration = Duration.ofSeconds(10);
  // health state number of windows
  // TODO make this configurable
  private static final int windowCount = 3;
  private final Ticker ticker;
  private final Map<TopicPartition, PipelineHealthState> stateMap;
  private final Job jobTemplate;

  private final Scope scope;

  /**
   * Instantiates a new Pipeline heath manager.
   *
   * @param ticker the ticker
   */
  PipelineHealthManager(Ticker ticker, Job jobTemplate, Scope scope) {
    this.ticker = ticker;
    this.jobTemplate = jobTemplate;
    this.scope = scope;
    this.stateMap = new ConcurrentHashMap<>();
  }

  /**
   * Reports a issue. the issue will be recorded in the health state window.
   *
   * @param job the job
   * @param issue the issue
   */
  public void reportIssue(Job job, PipelineHealthIssue issue) {
    TopicPartition tp =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    PipelineHealthState healthState = stateMap.get(tp);
    if (healthState != null) {
      healthState.recordIssue(issue);
    }
  }

  @VisibleForTesting
  protected Set<PipelineHealthIssue> getPipelineHealthIssues(Job job) {
    TopicPartition tp =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    PipelineHealthState state = stateMap.get(tp);
    if (state != null) {
      return state.getIssues();
    } else {
      return Collections.emptySet();
    }
  }

  public void init(Job job) {
    TopicPartition tp =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    stateMap.computeIfAbsent(
        tp, o -> new PipelineHealthState(ticker, stateWindowDuration, windowCount));
  }

  public void cancel(Job job) {
    TopicPartition tp =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    stateMap.remove(tp);
  }

  public void cancelAll() {
    stateMap.clear();
  }

  @Override
  public void publishMetrics() {
    stateMap.forEach(
        (tp, state) -> {
          Set<PipelineHealthIssue> issues = state.getIssues();
          StructuredTags structuredTags =
              StructuredTags.builder()
                  .setKafkaCluster(jobTemplate.getKafkaConsumerTask().getCluster())
                  .setKafkaGroup(jobTemplate.getKafkaConsumerTask().getConsumerGroup())
                  .setKafkaTopic(jobTemplate.getKafkaConsumerTask().getTopic())
                  .setKafkaPartition(tp.partition());
          Scope jobScope = scope.tagged(structuredTags.build());
          issues.forEach(
              issue -> {
                jobScope
                    .tagged(StructuredTags.builder().setError(issue.getName()).build())
                    .gauge(MetricNames.JOB_HEALTH_STATE)
                    .update(1);
              });
        });
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** The type Builder. */
  public static class Builder {
    private Ticker ticker = Ticker.systemTicker();

    private Scope scope = new NoopScope();

    /**
     * Sets ticker.
     *
     * @param ticker the ticker
     * @return the ticker
     */
    public Builder setTicker(Ticker ticker) {
      this.ticker = ticker;
      return this;
    }

    /**
     * Sets scope.
     *
     * @param scope the scope
     * @return the scope
     */
    public Builder setScope(Scope scope) {
      this.scope = scope;
      return this;
    }

    /**
     * Build pipeline heath manager.
     *
     * @return the pipeline heath manager
     */
    public PipelineHealthManager build(Job jobTemplate) {
      return new PipelineHealthManager(ticker, jobTemplate, scope);
    }
  }

  protected static class MetricNames {

    static final String JOB_HEALTH_STATE = "pipeline.health-manager.job-health-state";
  }
}
