package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.MessageOrBuilder;
import com.uber.data.kafka.datatransfer.AutoScalarSnapshot;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.JobGroup;
import com.uber.data.kafka.datatransfer.JobGroupScalarSnapshot;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.data.kafka.datatransfer.controller.rpc.Workload;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * AutoScalar 1. Aggregates job throughout into JobGroup throughput 2. Records job Group throughput
 * in timed bounded window 3. When window matured, use the information to update {@link
 * com.uber.data.kafka.datatransfer.ScaleStatus} of jobGroup Internal status cleanup automatically
 * after ttl since last access
 */
public class AutoScalar implements Scalar {

  private static final Logger logger = LoggerFactory.getLogger(AutoScalar.class);
  // in-memory auto-scalar status store
  private final Cache<JobGroupKey, JobGroupScaleStatus> autoScalarStatusStore;
  private final AutoScalarConfiguration config;
  private final JobWorkloadMonitor JobWorkloadMonitor;
  private final ScaleState.Builder stateBuilder;
  private final Scope scope;
  private final LeaderSelector leaderSelector;

  /**
   * Instantiates a new Auto scalar.
   *
   * @param config the config
   * @param JobWorkloadMonitor the job activity monitor
   * @param ticker the ticker
   * @param scope the scope
   * @param leaderSelector the leader selector
   */
  public AutoScalar(
      AutoScalarConfiguration config,
      JobWorkloadMonitor JobWorkloadMonitor,
      Ticker ticker,
      Scope scope,
      LeaderSelector leaderSelector) {
    this.autoScalarStatusStore =
        CacheBuilder.newBuilder()
            .expireAfterAccess(config.getJobStatusTTL().toMillis(), TimeUnit.MILLISECONDS)
            .ticker(ticker)
            .build();
    this.JobWorkloadMonitor = JobWorkloadMonitor;
    this.config = config;
    this.scope = scope;
    this.leaderSelector = leaderSelector;
    this.stateBuilder = ScaleState.newBuilder().withConfig(config).withTicker(ticker);
  }

  /** Update sample of throughput to window */
  @Scheduled(fixedDelayString = "${master.autoscalar.sampleInterval}")
  public void runSample() {
    if (!leaderSelector.isLeader()) {
      logger.debug("skipped runSample because current instance is not leader");
      return;
    }

    Stopwatch timer = scope.timer(MetricNames.AUTOSCALAR_RUN_SAMPLE_LATENCY).start();

    // according to
    // https://guava.dev/releases/19.0/api/docs/com/google/common/cache/CacheBuilder.html#expireAfterAccess(long,%20java.util.concurrent.TimeUnit)
    // enumeration over entries doesn't reset access time, so expired entries can still be
    // evict by the cache
    for (Map.Entry<JobGroupKey, JobGroupScaleStatus> entry :
        autoScalarStatusStore.asMap().entrySet()) {
      JobWorkloadMonitor.get(entry.getKey())
          .ifPresent(
              t -> {
                double sampleScale = workloadToScale(t);
                entry.getValue().sampleScale(sampleScale);
                scope
                    .tagged(
                        StructuredTags.builder()
                            .setKafkaGroup(entry.getKey().getGroup())
                            .setKafkaTopic(entry.getKey().getTopic())
                            .build())
                    .gauge(MetricNames.AUTOSCALAR_SAMPLED_SCALE)
                    .update(sampleScale);
              });
    }
    timer.stop();
  }

  /**
   * Applies auto scale result to job group - Skips non-running jobs When quota changed, update job
   * group scale status
   *
   * @param rebalancingJobGroup the rebalancing job group
   * @param defaultScale the default scale, apply if it's in dryRun Mode
   */
  @Override
  public void apply(RebalancingJobGroup rebalancingJobGroup, double defaultScale) {
    if (!leaderSelector.isLeader()) {
      logger.debug("skipped apply because current instance is not leader");
      throw new IllegalArgumentException("Not Leader");
    }

    JobState state = rebalancingJobGroup.getJobGroupState();
    JobGroup jobGroup = rebalancingJobGroup.getJobGroup();
    if (state != JobState.JOB_STATE_RUNNING) {
      logger.debug(
          "skipped apply because jobGroup is not running",
          StructuredLogging.jobGroupId(jobGroup.getJobGroupId()),
          StructuredLogging.kafkaTopic(jobGroup.getKafkaConsumerTaskGroup().getTopic()),
          StructuredLogging.kafkaCluster(jobGroup.getKafkaConsumerTaskGroup().getCluster()),
          StructuredLogging.kafkaGroup(jobGroup.getKafkaConsumerTaskGroup().getConsumerGroup()));
      return;
    }

    JobGroupKey jobGroupKey = JobGroupKey.of(jobGroup);
    final SignatureAndScale quota = new SignatureAndScale(jobGroup.getFlowControl());
    final Optional<Double> scale = rebalancingJobGroup.getScale();
    double newScale =
        autoScalarStatusStore
            .asMap()
            .computeIfAbsent(
                jobGroupKey,
                key ->
                    new JobGroupScaleStatus(
                        jobGroupKey, scale.isPresent() ? quota.build(scale.get()) : quota))
            .getScale(quota, jobGroup.getMiscConfig().getScaleResetEnabled());

    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaGroup(jobGroup.getKafkaConsumerTaskGroup().getConsumerGroup())
                .setKafkaTopic(jobGroup.getKafkaConsumerTaskGroup().getTopic())
                .build())
        .gauge(MetricNames.AUTOSCALAR_COMPUTED_SCALE)
        .update(newScale);

    // use default scale in dryRun mode
    // TODO: remove hardcoded topic group after root cause fixed see KAFEP-2386
    if (config.isDryRun()
        || (jobGroupKey.getGroup().equals("fulfillment-indexing-gateway")
            && jobGroupKey
                .getTopic()
                .equals("fulfillment-raw-transport-provider-session-state-changes"))) {
      newScale = defaultScale;
    }

    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaGroup(jobGroup.getKafkaConsumerTaskGroup().getConsumerGroup())
                .setKafkaTopic(jobGroup.getKafkaConsumerTaskGroup().getTopic())
                .build())
        .gauge(MetricNames.AUTOSCALAR_APPLIED_SCALE)
        .update(newScale);

    if (rebalancingJobGroup.updateScale(newScale, scaleToThroughput(newScale))) {
      logger.info(
          String.format("update jobGroup scale to %.2f", newScale),
          StructuredLogging.jobGroupId(jobGroup.getJobGroupId()),
          StructuredLogging.kafkaTopic(jobGroup.getKafkaConsumerTaskGroup().getTopic()),
          StructuredLogging.kafkaCluster(jobGroup.getKafkaConsumerTaskGroup().getCluster()),
          StructuredLogging.kafkaGroup(jobGroup.getKafkaConsumerTaskGroup().getConsumerGroup()));
    }
  }

  /**
   * Takes a dump of internal state of AutoScalar, including scale, vertical scale limit, time
   * window etc. this dump can be used for data analysis
   *
   * @return a snapshot of state
   */
  @Override
  public MessageOrBuilder snapshot() {
    return AutoScalarSnapshot.newBuilder()
        .addAllJobGroupScalar(
            autoScalarStatusStore.asMap().values().stream()
                .map(JobGroupScaleStatus::snapshot)
                .collect(Collectors.toList()))
        .build();
  }

  /**
   * Coverts observed workload to scale
   *
   * @param workload
   * @return
   */
  private double workloadToScale(Workload workload) {
    return Math.max(
        workload.getMessagesPerSecond() / config.getMessagesPerSecPerWorker(),
        workload.getBytesPerSecond() / config.getBytesPerSecPerWorker());
  }

  /**
   * Converts computed scale to computed throughput. This throughput represents the controller's
   * view of the throughput of the job group. It is not the same as the actual throughput of the job
   * group, but will be useful by another controller for job group scaling.
   *
   * @param scale the scale
   * @return the throughput
   */
  private Throughput scaleToThroughput(double scale) {
    return new Throughput(
        scale * config.getMessagesPerSecPerWorker(), scale * config.getBytesPerSecPerWorker());
  }

  @VisibleForTesting
  protected Map<JobGroupKey, JobGroupScaleStatus> getStatusStore() {
    return autoScalarStatusStore.asMap();
  }

  @VisibleForTesting
  protected void cleanUp() {
    autoScalarStatusStore.cleanUp();
  }

  /**
   * Converts quota to scale.
   *
   * @param flowControl
   * @return
   */
  private double quotaToScale(FlowControl flowControl) {
    return Math.max(
        flowControl.getMessagesPerSec() / config.getMessagesPerSecPerWorker(),
        flowControl.getBytesPerSec() / config.getBytesPerSecPerWorker());
  }

  /**
   * calculate hash value of quota
   *
   * @param flowControl
   * @return
   */
  private int quotaToHash(FlowControl flowControl) {
    return Objects.hash(flowControl.getMessagesPerSec(), flowControl.getBytesPerSec());
  }

  @VisibleForTesting
  protected class JobGroupScaleStatus {
    private final JobGroupKey jobGroupKey;
    private ScaleState state;
    private long signature;

    JobGroupScaleStatus(JobGroupKey jobGroupKey, SignatureAndScale scale) {
      this.jobGroupKey = jobGroupKey;
      reset(scale);
    }

    private void reset(SignatureAndScale scale) {
      signature = scale.signature;
      state = stateBuilder.build(scale.scale);
    }

    /**
     * Gets proposed scale of a job group
     *
     * @param quota the quota of a job group
     * @param scaleResetEnabled the flag to control scale reset
     * @return the scale
     */
    private double getScale(SignatureAndScale quota, boolean scaleResetEnabled) {
      // reset scale when flow control updated and scale reset is enabled
      if (signature != quota.signature && scaleResetEnabled) {
        synchronized (this) {
          if (signature != quota.signature) {
            reset(quota);
          }
        }
      }

      return state.getScale();
    }

    /**
     * Take a sample of scale.
     *
     * @param sampleScale current observed sampleScale of a job group
     */
    synchronized void sampleScale(double sampleScale) {
      state = state.onSample(sampleScale);
    }

    /**
     * Takes a dump of internal state, including job group key, vertical scale state, etc for data
     * analysis
     *
     * @return a dump of internal state
     */
    public JobGroupScalarSnapshot snapshot() {
      return JobGroupScalarSnapshot.newBuilder()
          .setConsumerGroup(jobGroupKey.getGroup())
          .setTopic(jobGroupKey.getTopic())
          .setCluster(jobGroupKey.getCluster())
          .setScaleStateSnapshot(state.snapshot())
          .build();
    }
  }

  /**
   * SignatureAndScale represent scale with signature generated from flowControl setting. so that
   * flowControl update can invalidate and reset scale
   */
  private class SignatureAndScale {
    private final double scale;
    private final long signature;

    private SignatureAndScale(FlowControl flowControl) {
      this(quotaToScale(flowControl), quotaToHash(flowControl));
    }

    private SignatureAndScale(double scale, long signature) {
      this.scale = scale;
      this.signature = signature;
    }

    /**
     * build new instance with new scale value
     *
     * @param newScale
     * @return
     */
    private SignatureAndScale build(double newScale) {
      return new SignatureAndScale(newScale, signature);
    }
  }

  private static class MetricNames {
    private static final String AUTOSCALAR_RUN_SAMPLE_LATENCY = "autoscalar.run.sample.latency";
    private static final String AUTOSCALAR_COMPUTED_SCALE = "autoscalar.computed.scale";
    private static final String AUTOSCALAR_APPLIED_SCALE = "autoscalar.applied.scale";
    private static final String AUTOSCALAR_SAMPLED_SCALE = "autoscalar.sampled.scale";
  }
}
