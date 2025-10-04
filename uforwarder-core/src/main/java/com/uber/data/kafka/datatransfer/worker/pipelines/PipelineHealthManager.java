package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.uber.data.kafka.datatransfer.Job;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;

/** PipelineHealthManager tracks and reports health issue of a pipeline. */
public class PipelineHealthManager {
  // heath state window duration
  // TODO make this configurable
  private static final Duration stateWindowDuration = Duration.ofSeconds(10);
  // health state number of windows
  // TODO make this configurable
  private static final int windowCount = 3;
  private final Ticker ticker;
  private final Map<TopicPartition, PipelineHealthState> stateMap;
  private final Job jobTemplate;

  /**
   * Instantiates a new Pipeline heath manager.
   *
   * @param ticker the ticker
   */
  PipelineHealthManager(Ticker ticker, Job jobTemplate) {
    this.ticker = ticker;
    this.jobTemplate = jobTemplate;
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
  protected int getPipelineHealthStateValue(Job job) {
    TopicPartition tp =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    PipelineHealthState state = stateMap.get(tp);
    if (state != null) {
      return state.getStateValue();
    } else {
      return 0;
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

  public static Builder newBuilder() {
    return new Builder();
  }

  /** The type Builder. */
  public static class Builder {
    private Ticker ticker = Ticker.systemTicker();

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
     * Build pipeline heath manager.
     *
     * @return the pipeline heath manager
     */
    public PipelineHealthManager build(Job jobTemplate) {
      return new PipelineHealthManager(ticker, jobTemplate);
    }
  }
}
