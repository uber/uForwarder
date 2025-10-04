package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.facebook.infer.annotation.ThreadSafe;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.CommandType;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.JobStatus;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.KafkaConsumerTaskStatus;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.data.kafka.datatransfer.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.data.kafka.datatransfer.common.utils.ShutdownableThread;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.datatransfer.worker.common.TracedConsumerRecord;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import com.uber.m3.util.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractKafkaFetcherThread implements the framework of creating and initializing Kafka Consumer,
 * updating assigned topic partitions, fetching messages from Kafka servers, sending them to the
 * next stage, committing offsets to Kafka servers and so on.
 *
 * <p>It leaves subclasses the ability to decide how to decide start and end offset.
 *
 * @param <K> the key type for {@link ConsumerRecord}
 * @param <V> the value type for {@link ConsumerRecord}
 */
public abstract class AbstractKafkaFetcherThread<K, V> extends ShutdownableThread {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaFetcherThread.class);

  // we don't need to change the metric report interval
  private static final int PARTITION_OWNERSHIP_REPORT_INTERVAL_MS = 1000;

  // For idle fetcher thread, sleep between each poll request to save CPU cycles
  private static final int FETCHER_IDLE_SLEEP_MS = 1000;

  // The cool down window that is used for more aggressive offset commit action (i.e commit offset
  // without offset movement)
  private static final long ACTIVE_COMMIT_INTERVAL_IN_MS = TimeUnit.SECONDS.toMillis(60);

  // As its name indicates, previousJobRunningMap is used to save the previous jobRunningMap.
  // The refreshPartitionMap() method compares the current jobRunningMap and the
  // previousJobRunningMap
  // to decide whether topic-partitions assigned to this consumer has changed or not.
  //
  // After each refreshPartitionMap(), previousJobRunningMap will be changed to the jobRunningMap.
  final ConcurrentMap<Long, Job> currentRunningJobMap = new ConcurrentHashMap<>();

  // a scheduled executor service for reporting metric
  private final ScheduledExecutorService scheduledExecutorService;

  private final Consumer<K, V> kafkaConsumer;

  // last logTopicPartitionInfo time
  private long lastDumpTime = 0L;
  // try to do commit offset for each record, commit will only happen every minute
  // (PER_RECORD_COMMIT_INTERVAL_IN_MS)
  private final boolean perRecordCommit;

  // time interval to log topic partition offset lag
  private final int offsetMonitorMs;

  // The time spend waiting spent waiting in poll if data is not available in the buffer
  private final int pollTimeoutMs;

  private final CheckpointManager checkpointManager;
  private final KafkaFetcherConfiguration config;
  private final ThroughputTracker throughputTracker;
  private final DelayProcessManager delayProcessManager;

  private final InflightMessageTracker inflightMessageTracker;

  private final Scope scope;
  private final CoreInfra infra;

  // whether to commit offset asynchronously. Async commit will not block consumer poll request,
  // hence the throughput is higher
  private final boolean asyncCommitOffset;
  @Nullable private volatile Sink<ConsumerRecord<K, V>, Long> processorSink;
  @Nullable private volatile PipelineStateManager pipelineStateManager;
  private long lastCommitTimestampMs = -1L;
  private long lastCommitOffsetsCheckTimeMs = -1L;

  public AbstractKafkaFetcherThread(
      String threadName,
      KafkaFetcherConfiguration config,
      CheckpointManager checkpointManager,
      ThroughputTracker throughputTracker,
      InflightMessageTracker inflightMessageTracker,
      Consumer<K, V> kafkaConsumer,
      CoreInfra infra,
      boolean asyncCommitOffset) {
    this(
        threadName,
        config,
        checkpointManager,
        throughputTracker,
        DelayProcessManager.NOOP,
        inflightMessageTracker,
        kafkaConsumer,
        infra,
        asyncCommitOffset,
        false);
  }

  public AbstractKafkaFetcherThread(
      String threadName,
      KafkaFetcherConfiguration config,
      CheckpointManager checkpointManager,
      ThroughputTracker throughputTracker,
      DelayProcessManager<K, V> delayProcessManager,
      InflightMessageTracker inflightMessageTracker,
      Consumer<K, V> kafkaConsumer,
      CoreInfra infra,
      boolean asyncCommitOffset,
      boolean perRecordCommit) {
    super(threadName, true);
    this.scheduledExecutorService =
        infra.contextManager().wrap(Executors.newSingleThreadScheduledExecutor());
    this.checkpointManager = checkpointManager;
    this.throughputTracker = throughputTracker;
    this.delayProcessManager = delayProcessManager;
    this.inflightMessageTracker = inflightMessageTracker;
    this.config = config;
    this.offsetMonitorMs = config.getOffsetMonitorIntervalMs();
    this.pollTimeoutMs = config.getPollTimeoutMs();
    this.kafkaConsumer = kafkaConsumer;
    this.scope =
        infra
            .scope()
            .tagged(
                StructuredTags.builder().setFetcherType(this.getClass().getSimpleName()).build());
    this.infra = infra;
    this.asyncCommitOffset = asyncCommitOffset;
    this.perRecordCommit = perRecordCommit;
  }

  public void setNextStage(Sink<ConsumerRecord<K, V>, Long> sink) {
    processorSink = sink;
  }

  public void setPipelineStateManager(PipelineStateManager pipelineStateManager) {
    this.pipelineStateManager = pipelineStateManager;
  }

  @Override
  public synchronized void start() {
    Preconditions.checkNotNull(processorSink, "sink required");
    Preconditions.checkNotNull(checkpointManager, "checkpoint manager required");
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    scheduledExecutorService.schedule(
        this::reportPartitionOwnership,
        PARTITION_OWNERSHIP_REPORT_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
    super.start();
  }

  private void reportPartitionOwnership() {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    pipelineStateManager
        .getExpectedRunningJobMap()
        .values()
        .forEach(
            job ->
                scope
                    .tagged(
                        StructuredTags.builder()
                            .setKafkaGroup(job.getKafkaConsumerTask().getConsumerGroup())
                            .setKafkaTopic(job.getKafkaConsumerTask().getTopic())
                            .setKafkaPartition(job.getKafkaConsumerTask().getPartition())
                            .build())
                    .gauge(MetricNames.TOPIC_PARTITION_OWNED_EXPECTED)
                    .update(1.0));
    pipelineStateManager
        .getJobStatus()
        .forEach(
            jobStatus ->
                scope
                    .tagged(
                        StructuredTags.builder()
                            .setKafkaGroup(
                                jobStatus.getJob().getKafkaConsumerTask().getConsumerGroup())
                            .setKafkaTopic(jobStatus.getJob().getKafkaConsumerTask().getTopic())
                            .setKafkaPartition(
                                jobStatus.getJob().getKafkaConsumerTask().getPartition())
                            .build())
                    .gauge(MetricNames.TOPIC_PARTITION_OWNED_ACTUAL)
                    .update(1.0));
    scheduledExecutorService.schedule(
        this::reportPartitionOwnership,
        PARTITION_OWNERSHIP_REPORT_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
  }

  @Override
  public void doWork() {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    Preconditions.checkNotNull(processorSink, "processor sink required");
    if (!processorSink.isRunning()) {
      initiateShutdown();
      cleanup();
      return;
    }
    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaCluster(
                    pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getCluster())
                .setKafkaGroup(
                    pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getConsumerGroup())
                .setKafkaTopic(
                    pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getTopic())
                .build())
        .gauge(MetricNames.FETCHER_KAFKA_WORKING)
        .update(1.0);
    // step 1: discover topic partition changes
    Map<TopicPartition, Job> addedTopicPartitionJobMap = new HashMap<>();
    Map<TopicPartition, Job> removedTopicPartitionJobMap = new HashMap<>();
    Map<TopicPartition, Job> allTopicPartitionJobMap = new HashMap<>();
    // the extractTopicPartitionMap method will modify three passed-in maps, therefore, do not make
    // those maps immutable.
    boolean assignmentChange =
        extractTopicPartitionMap(
            addedTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap);

    // step 2: handle newly-added topic partitions
    // specifically, (1) the checkpoint manager starts to track them (2) extracts topic partitions
    // with non-negative start offset for further processing.
    // (3) fetch committed offset from broker and put it into checkpoint manager
    Map<TopicPartition, OffsetAndMetadata> brokerCommittedOffset = new HashMap<>();
    if (!addedTopicPartitionJobMap.isEmpty()) {
      brokerCommittedOffset = getBrokerCommittedOffset(addedTopicPartitionJobMap.keySet());
    }
    Map<TopicPartition, Long> topicPartitionOffsetMap =
        addToCheckPointManager(addedTopicPartitionJobMap, brokerCommittedOffset);

    // step 2.1 track throughput and inflightMessage of newly added topic partitions and the removed
    // topic partitions
    adjustTracker(addedTopicPartitionJobMap, removedTopicPartitionJobMap);

    // use kafkaConsumerLock to ensure threadsafe access for kafkaConsumer and the in-order enqueue
    // for ConsumerRecords
    try {
      // step 3: performs topic partition addition and deletion
      // step 3.1: update assignment
      if (assignmentChange) {
        LOGGER.info(
            "assignment changed",
            StructuredLogging.kafkaTopicPartitions(allTopicPartitionJobMap.keySet()));
        kafkaConsumer.assign(allTopicPartitionJobMap.keySet());

        // delete removed jobs from delayProcessManager
        delayProcessManager.delete(removedTopicPartitionJobMap.keySet());

        // Resume all topic partitions in case they are paused before.
        // The partitions paused by delayProcessManager will not be resumed here, but will be
        // resumed in DelayProcessManager.
        // If we don't resume and if the previously paused topic partitions are assigned to
        // this worker again, those topic partitions will still be in paused state. This might be
        // a problem because we cannot fetch messages from this specific topic partitions.
        // topic partitions paused by delayProcessManager will be resumed in resumeTopicPartitions
        // function.
        List<TopicPartition> pausedTopicPartitions = delayProcessManager.getAll();
        Collection<TopicPartition> topicPartitions =
            allTopicPartitionJobMap.keySet().stream()
                .filter(tp -> !pausedTopicPartitions.contains(tp))
                .collect(Collectors.toSet());
        kafkaConsumer.resume(topicPartitions);
      }

      // step 3.2: seek start offsets when it's necessary
      seekStartOffset(topicPartitionOffsetMap, allTopicPartitionJobMap);

      // step 4: commits offsets
      commitOffsets(allTopicPartitionJobMap);

      // step 5: poll messages
      @Nullable ConsumerRecords<K, V> records = pollMessages(allTopicPartitionJobMap);

      // step 6: get resumed topic partitions
      Map<TopicPartition, List<ConsumerRecord<K, V>>> resumedRecords =
          delayProcessManager.resumePausedPartitionsAndRecords();

      // step 7: merge resumed records with new polled records
      @Nullable ConsumerRecords<K, V> mergedRecords = mergeRecords(records, resumedRecords);

      // step 8: update actual running job status here because we use async commit, so offsets are
      // committed to Kafka servers when we are doing polling.
      updateActualRunningJobStatus();

      // step 9: process messages
      processFetchedData(mergedRecords, allTopicPartitionJobMap);

      // step 10: log and report metric
      logTopicPartitionOffsetInfo(allTopicPartitionJobMap);
    } catch (Throwable e) {
      // TODO (T4575853): recreate kafka consumer if kafka consumer is closed.
      scope
          .tagged(
              StructuredTags.builder()
                  .setKafkaCluster(
                      pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getCluster())
                  .setKafkaGroup(
                      pipelineStateManager
                          .getJobTemplate()
                          .getKafkaConsumerTask()
                          .getConsumerGroup())
                  .setKafkaTopic(
                      pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getTopic())
                  .setError(e)
                  .build())
          .gauge(MetricNames.FETCHER_KAFKA_NOT_WORKING)
          .update(1.0);
      // don't log authorization exception because it will be in large volume
      if (!(e instanceof TopicAuthorizationException)) {
        LOGGER.error("failed to doWork", e);
      }
      // reset offset to fetchOffset to avoid data loss
      for (Map.Entry<TopicPartition, Job> entry : allTopicPartitionJobMap.entrySet()) {
        if (checkpointManager.getCheckpointInfo(entry.getValue()).getFetchOffset() >= 0) {
          kafkaConsumer.seek(
              entry.getKey(),
              checkpointManager.getCheckpointInfo(entry.getValue()).getFetchOffset());
        }
      }
    }
  }

  @VisibleForTesting
  Map<TopicPartition, OffsetAndMetadata> getBrokerCommittedOffset(
      Set<TopicPartition> addedTopicPartition) {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    Stopwatch kafkaCommittedTimer =
        scope
            .tagged(
                StructuredTags.builder()
                    .setKafkaGroup(
                        pipelineStateManager
                            .getJobTemplate()
                            .getKafkaConsumerTask()
                            .getConsumerGroup())
                    .setKafkaTopic(
                        pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getTopic())
                    .build())
            .timer(MetricNames.OFFSET_FETCH_LATENCY)
            .start();
    try {
      return kafkaConsumer.committed(addedTopicPartition);
    } catch (Throwable e) {
      LOGGER.error("failed to fetch committed offset from kafka servers", e);
      scope
          .tagged(
              StructuredTags.builder()
                  .setKafkaGroup(
                      pipelineStateManager
                          .getJobTemplate()
                          .getKafkaConsumerTask()
                          .getConsumerGroup())
                  .setKafkaTopic(
                      pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getTopic())
                  .build())
          .counter(MetricNames.OFFSET_FETCH_EXCEPTION)
          .inc(1);
      return ImmutableMap.of();
    } finally {
      kafkaCommittedTimer.stop();
    }
  }

  @VisibleForTesting
  @ThreadSafe(enableChecks = false)
  void commitOffsets(Map<TopicPartition, Job> allTopicPartitionJobMap) {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    if (allTopicPartitionJobMap.isEmpty()) {
      return;
    }
    long currentTimestampMs = System.currentTimeMillis();
    // we don't need to make lastCommitOffsetsCheckTimeMs thread-safe, as this number does not need
    // to be
    // accurate
    if (currentTimestampMs - lastCommitOffsetsCheckTimeMs > config.getOffsetCommitIntervalMs()) {
      lastCommitOffsetsCheckTimeMs = currentTimestampMs;
      try {
        Map<TopicPartition, OffsetAndMetadata> tpCommitInfoMap = new HashMap<>();
        allTopicPartitionJobMap.forEach(
            (tp, job) -> {
              long offsetToCommit = checkpointManager.getOffsetToCommit(job);
              long committedOffset = checkpointManager.getCommittedOffset(job);
              if (eligibleToCommit(offsetToCommit, committedOffset)) {
                tpCommitInfoMap.put(tp, new OffsetAndMetadata(offsetToCommit));
              }
            });
        if (tpCommitInfoMap.isEmpty()) {
          return;
        }
        String consumerGroup =
            pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getConsumerGroup();
        String topic = pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getTopic();
        if (asyncCommitOffset) {
          commitAsync(tpCommitInfoMap, allTopicPartitionJobMap, consumerGroup, topic);
        } else {
          commitSync(tpCommitInfoMap, allTopicPartitionJobMap, consumerGroup, topic);
        }
        lastCommitTimestampMs = currentTimestampMs;
      } catch (Throwable throwable) {
        // we catch the error and continue the work
        LOGGER.error("failed to commit offsets to kafka servers", throwable);
        scope.counter(MetricNames.OFFSET_COMMIT_EXCEPTION).inc(1);
      }
    }
  }

  @Nullable
  ConsumerRecords<K, V> mergeRecords(
      @Nullable ConsumerRecords<K, V> records,
      Map<TopicPartition, List<ConsumerRecord<K, V>>> resumedRecords) {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");

    if (resumedRecords == null || resumedRecords.isEmpty()) {
      return records;
    }

    if (records == null) {
      return new ConsumerRecords<>(resumedRecords);
    }

    Map<TopicPartition, List<ConsumerRecord<K, V>>> mergedRecords = new HashMap<>(resumedRecords);
    for (TopicPartition partition : records.partitions()) {
      if (resumedRecords.containsKey(partition)) {
        LOGGER.warn(
            "The delay partitions may have duplications",
            StructuredLogging.kafkaGroup(
                pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getConsumerGroup()),
            StructuredLogging.kafkaTopic(partition.topic()),
            StructuredLogging.kafkaPartition(partition.partition()));
        scope
            .tagged(
                StructuredTags.builder()
                    .setKafkaGroup(
                        pipelineStateManager
                            .getJobTemplate()
                            .getKafkaConsumerTask()
                            .getConsumerGroup())
                    .setKafkaTopic(partition.topic())
                    .setKafkaPartition(partition.partition())
                    .build())
            .gauge(MetricNames.TOPIC_PARTITION_DELAY_DUPLICATION)
            .update(resumedRecords.get(partition).size());
      }
      mergedRecords
          .computeIfAbsent(partition, key -> new ArrayList<>())
          .addAll(records.records(partition));
    }
    return new ConsumerRecords<>(mergedRecords);
  }

  private boolean eligibleToCommit(long offsetToCommit, long committedOffset) {
    if (offsetToCommit == committedOffset
        && (System.currentTimeMillis() - lastCommitTimestampMs > ACTIVE_COMMIT_INTERVAL_IN_MS)) {
      return true;
    } else if (offsetToCommit != committedOffset
        && offsetToCommit > KafkaUtils.MAX_INVALID_OFFSET_TO_COMMIT) {
      return true;
    }
    return false;
  }

  private void commitSync(
      Map<TopicPartition, OffsetAndMetadata> tpCommitInfoMap,
      Map<TopicPartition, Job> allTopicPartitionJobMap,
      String consumerGroup,
      String topic) {
    final Scope scopeWithClusterGroupTopic =
        scope.tagged(
            StructuredTags.builder().setKafkaGroup(consumerGroup).setKafkaTopic(topic).build());
    Stopwatch commitStopwatch =
        scopeWithClusterGroupTopic.timer(MetricNames.OFFSET_COMMIT_LATENCY).start();
    try {
      kafkaConsumer.commitSync(tpCommitInfoMap);
      onCommitCompletion(tpCommitInfoMap, Optional.empty(), allTopicPartitionJobMap, consumerGroup);
    } catch (Exception e) {
      onCommitCompletion(tpCommitInfoMap, Optional.of(e), allTopicPartitionJobMap, consumerGroup);
    } finally {
      commitStopwatch.stop();
    }
  }

  // async commit offset to kafka broker
  @ThreadSafe(enableChecks = false)
  private void commitAsync(
      Map<TopicPartition, OffsetAndMetadata> tpCommitInfoMap,
      Map<TopicPartition, Job> allTopicPartitionJobMap,
      String consumerGroup,
      String topic) {
    final Scope scopeWithClusterGroupTopic =
        scope.tagged(
            StructuredTags.builder().setKafkaGroup(consumerGroup).setKafkaTopic(topic).build());
    Stopwatch commitStopwatch =
        scopeWithClusterGroupTopic.timer(MetricNames.OFFSET_COMMIT_LATENCY).start();
    kafkaConsumer.commitAsync(
        tpCommitInfoMap,
        (offsets, exception) -> {
          commitStopwatch.stop();
          onCommitCompletion(
              offsets, Optional.ofNullable(exception), allTopicPartitionJobMap, consumerGroup);
        });
  }

  private void onCommitCompletion(
      Map<TopicPartition, OffsetAndMetadata> offsets,
      Optional<Exception> exception,
      Map<TopicPartition, Job> allTopicPartitionJobMap,
      String consumerGroup) {
    if (!exception.isPresent()) {
      LOGGER.debug("committed offsets", StructuredLogging.count(offsets.size()));
      offsets.forEach(
          (tp, offsetMeta) -> {
            Job job = allTopicPartitionJobMap.get(tp);
            if (job == null) {
              return;
            }
            checkpointManager.setCommittedOffset(job, offsetMeta.offset());
            LOGGER.debug(
                "successfully committed offset",
                StructuredLogging.kafkaGroup(consumerGroup),
                StructuredLogging.kafkaTopic(tp.topic()),
                StructuredLogging.kafkaPartition(tp.partition()),
                StructuredLogging.kafkaOffset(offsetMeta.offset()));

            final Scope scopeWithGroupTopicPartition =
                scope.tagged(
                    StructuredTags.builder()
                        .setKafkaGroup(consumerGroup)
                        .setKafkaTopic(tp.topic())
                        .setKafkaPartition(tp.partition())
                        .build());
            scopeWithGroupTopicPartition.counter(MetricNames.OFFSET_COMMIT_SUCCESS).inc(1);
            scopeWithGroupTopicPartition.gauge(MetricNames.OFFSET).update(offsetMeta.offset());
          });
    } else {
      offsets.forEach(
          (tp, offsetMeta) -> {
            LOGGER.error(
                "failed to commit offset",
                StructuredLogging.kafkaGroup(consumerGroup),
                StructuredLogging.kafkaTopic(tp.topic()),
                StructuredLogging.kafkaPartition(tp.partition()),
                StructuredLogging.kafkaOffset(offsetMeta.offset()),
                exception.get());
            final Scope scopeWithGroupTopicPartition =
                scope.tagged(
                    StructuredTags.builder()
                        .setKafkaGroup(consumerGroup)
                        .setKafkaTopic(tp.topic())
                        .setKafkaPartition(tp.partition())
                        .build());
            scopeWithGroupTopicPartition.counter(MetricNames.OFFSET_COMMIT_FAILURE).inc(1);
          });
    }
  }

  @VisibleForTesting
  boolean extractTopicPartitionMap(
      Map<TopicPartition, Job> newTopicPartitionJobMap,
      Map<TopicPartition, Job> removedTopicPartitionJobMap,
      Map<TopicPartition, Job> allTopicPartitionJobMap) {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    // Use a snapshot of the expectedRunningJobMap as it might change.
    Map<Long, Job> expectedRunningJobMap = pipelineStateManager.getExpectedRunningJobMap();
    // step 1: create an old topic partition set and an old job set from previousJobRunningMap.
    Map<Long, Job> oldJobs = ImmutableMap.copyOf(currentRunningJobMap);
    Set<TopicPartition> oldTPs = new HashSet<>();
    currentRunningJobMap.forEach(
        (jobId, job) -> {
          TopicPartition topicPartition =
              new TopicPartition(
                  job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
          oldTPs.add(topicPartition);
        });
    currentRunningJobMap.clear();
    currentRunningJobMap.putAll(expectedRunningJobMap);

    // step 2: create allTopicPartitionJobMap from current running jobSet
    expectedRunningJobMap.forEach(
        (jobId, job) -> {
          // TODO (T4367183): currently, if more than one jobs have the same TopicPartition,
          //  randomly pick one. Need to think about how to merge jobs correctly carefully
          TopicPartition topicPartition =
              new TopicPartition(
                  job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
          if (allTopicPartitionJobMap.containsKey(topicPartition)) {
            LOGGER.error(
                "two jobs have the same topic partition",
                StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
                StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
                StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
                StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()),
                StructuredLogging.reason(
                    "two job ids: "
                        + job.getJobId()
                        + ", "
                        + allTopicPartitionJobMap.get(topicPartition).getJobId()));
            scope
                .tagged(
                    StructuredTags.builder()
                        .setKafkaGroup(job.getKafkaConsumerTask().getConsumerGroup())
                        .setKafkaTopic(job.getKafkaConsumerTask().getTopic())
                        .setKafkaPartition(job.getKafkaConsumerTask().getPartition())
                        .build())
                .gauge(MetricNames.TOPIC_PARTITION_DUP_JOB)
                .update(1.0);
          } else {
            allTopicPartitionJobMap.put(topicPartition, job);
          }
        });

    // step 3: check for new topic partitions
    for (Map.Entry<TopicPartition, Job> entry : allTopicPartitionJobMap.entrySet()) {
      // do not need to re-seek offset for existing topic-partitions
      if (!oldTPs.contains(entry.getKey())) {
        newTopicPartitionJobMap.put(entry.getKey(), entry.getValue());
      }
    }

    List<Job> runJobs = new ArrayList<>();
    for (Map.Entry<Long, Job> jobEntry : expectedRunningJobMap.entrySet()) {
      if (!oldJobs.containsKey(jobEntry.getKey())) {
        runJobs.add(jobEntry.getValue());
      }
    }

    // step 4: add cancel jobs to removedTopicPartitionJobMap
    List<Job> cancelJobs = new ArrayList<>();
    for (Map.Entry<Long, Job> jobEntry : oldJobs.entrySet()) {
      if (!expectedRunningJobMap.containsKey(jobEntry.getKey())) {
        Job cancelJob = jobEntry.getValue();
        cancelJobs.add(cancelJob);
        removedTopicPartitionJobMap.put(
            new TopicPartition(
                cancelJob.getKafkaConsumerTask().getTopic(),
                cancelJob.getKafkaConsumerTask().getPartition()),
            cancelJob);
      }
    }

    logCommands(runJobs, cancelJobs);

    // step 5: Check for job assignment changes.
    return !runJobs.isEmpty() || !cancelJobs.isEmpty();
  }

  private static void logCommands(List<Job> runJobs, List<Job> cancelJobs) {
    runJobs
        .iterator()
        .forEachRemaining(
            job -> {
              LOGGER.info(
                  "{} on kafka fetcher",
                  CommandType.COMMAND_TYPE_RUN_JOB,
                  StructuredLogging.jobId(job.getJobId()),
                  StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
                  StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
                  StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
                  StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()));
            });
    cancelJobs
        .iterator()
        .forEachRemaining(
            job -> {
              LOGGER.info(
                  "{} on kafka fetcher",
                  CommandType.COMMAND_TYPE_CANCEL_JOB,
                  StructuredLogging.jobId(job.getJobId()),
                  StructuredLogging.kafkaTopic(job.getKafkaConsumerTask().getTopic()),
                  StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
                  StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
                  StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()));
            });
  }

  void adjustTracker(
      Map<TopicPartition, Job> addedTopicPartitionJobMap,
      Map<TopicPartition, Job> removedTopicPartitionJobMap) {
    for (Map.Entry<TopicPartition, Job> entry : addedTopicPartitionJobMap.entrySet()) {
      inflightMessageTracker.init(entry.getKey());
      throughputTracker.init(entry.getValue());
    }

    for (TopicPartition tp : removedTopicPartitionJobMap.keySet()) {
      inflightMessageTracker.revokeInflightStatsForJob(tp);
    }
  }

  @VisibleForTesting
  Map<TopicPartition, Long> addToCheckPointManager(
      Map<TopicPartition, Job> addedTopicPartitionJobMap,
      Map<TopicPartition, OffsetAndMetadata> brokerCommittedOffset) {
    Map<TopicPartition, Long> startingOffsetMap = new HashMap<>();
    if (!addedTopicPartitionJobMap.isEmpty()) {
      for (Map.Entry<TopicPartition, Job> entry : addedTopicPartitionJobMap.entrySet()) {
        CheckpointInfo checkpointInfo = checkpointManager.addCheckpointInfo(entry.getValue());
        // if the offset to commit is not set, we set it to the broker committed offset
        if (!checkpointInfo.isCommitOffsetExists()
            && brokerCommittedOffset != null
            && brokerCommittedOffset.containsKey(entry.getKey())) {
          OffsetAndMetadata brokerOffset = brokerCommittedOffset.get(entry.getKey());
          // OffsetAndMetadata returned by kafka could be null
          if (brokerOffset == null) {
            continue;
          }
          checkpointInfo.setCommittedOffset(brokerOffset.offset());
          checkpointInfo.setOffsetToCommit(brokerOffset.offset());
          LOGGER.info(
              "set committed offset to broker committed offset",
              StructuredLogging.kafkaGroup(
                  entry.getValue().getKafkaConsumerTask().getConsumerGroup()),
              StructuredLogging.kafkaTopic(entry.getValue().getKafkaConsumerTask().getTopic()),
              StructuredLogging.kafkaPartition(
                  entry.getValue().getKafkaConsumerTask().getPartition()),
              StructuredLogging.kafkaOffset(brokerOffset.offset()));
        }

        KafkaConsumerTask config = entry.getValue().getKafkaConsumerTask();
        long startingOffset = config.getStartOffset();
        // only seek start offset when it's a valid offset
        if (startingOffset >= 0) {
          startingOffsetMap.put(entry.getKey(), startingOffset);
        }
        if (startingOffset >= 0 || config.getEndOffset() > 0) {
          KafkaConsumerTask task = entry.getValue().getKafkaConsumerTask();
          Scope scopeWithGroupTopicPartition =
              scope.tagged(
                  StructuredTags.builder()
                      .setKafkaGroup(task.getConsumerGroup())
                      .setKafkaTopic(task.getTopic())
                      .setKafkaPartition(task.getPartition())
                      .build());
          scopeWithGroupTopicPartition
              .gauge(MetricNames.TOPIC_PARTITION_OFFSET_START)
              .update(config.getStartOffset());
          scopeWithGroupTopicPartition
              .gauge(MetricNames.TOPIC_PARTITION_OFFSET_END)
              .update(config.getEndOffset());
        }
      }
    }
    return startingOffsetMap;
  }

  @VisibleForTesting
  public void seekStartOffset(
      Map<TopicPartition, Long> seekOffsetTaskMap, Map<TopicPartition, Job> topicPartitionJobMap) {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    if (seekOffsetTaskMap.isEmpty()) {
      return;
    }
    LOGGER.info(
        "seek start offsets",
        StructuredLogging.kafkaTopicPartitions(topicPartitionJobMap.keySet()));
    Map<TopicPartition, Long> beginningOffsets =
        kafkaConsumer.beginningOffsets(seekOffsetTaskMap.keySet());
    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(seekOffsetTaskMap.keySet());
    for (Map.Entry<TopicPartition, Long> entry : seekOffsetTaskMap.entrySet()) {
      TopicPartition tp = entry.getKey();
      long offset = entry.getValue();
      if (topicPartitionJobMap.get(tp) == null) {
        continue;
      }
      if (offset >= 0) {
        Long earliestOffset = beginningOffsets.get(tp);
        Long latestOffset = endOffsets.get(tp);
        String consumerGroup =
            pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getConsumerGroup();
        // log the offset info
        if (earliestOffset == null) {
          logStartOffsetInfo(
              consumerGroup,
              tp.topic(),
              tp.partition(),
              offset,
              MetricNames.TOPIC_PARTITION_SEEK_START_NULL);
        } else if (earliestOffset > offset) {
          logStartOffsetInfo(
              consumerGroup,
              tp.topic(),
              tp.partition(),
              offset,
              MetricNames.TOPIC_PARTITION_SEEK_TOO_SMALL);
        }
        if (latestOffset == null) {
          logStartOffsetInfo(
              consumerGroup,
              tp.topic(),
              tp.partition(),
              offset,
              MetricNames.TOPIC_PARTITION_SEEK_END_NULL);
        } else if (latestOffset < offset) {
          logStartOffsetInfo(
              consumerGroup,
              tp.topic(),
              tp.partition(),
              offset,
              MetricNames.TOPIC_PARTITION_SEEK_TOO_LARGE);
        }
        switch (getSeekStartOffsetOption(
            offset,
            earliestOffset,
            latestOffset,
            topicPartitionJobMap.get(tp).getKafkaConsumerTask().getAutoOffsetResetPolicy())) {
          case SEEK_TO_SPECIFIED_OFFSET:
            kafkaConsumer.seek(tp, offset);
            checkpointManager.setFetchOffset(topicPartitionJobMap.get(tp), offset);
            break;
          case SEEK_TO_EARLIEST_OFFSET:
            kafkaConsumer.seekToBeginning(Collections.singleton(tp));
            if (earliestOffset != null) {
              // it should be okay if we don't change the fetch offset, as it will eventually be
              // override when messages are being processed.
              checkpointManager.setFetchOffset(topicPartitionJobMap.get(tp), earliestOffset);
            }
            break;
          case SEEK_TO_LATEST_OFFSET:
            kafkaConsumer.seekToEnd(Collections.singleton(tp));
            if (latestOffset != null) {
              // it should be okay if we don't change the fetch offset, as it will eventually be
              // override when messages are being processed.
              checkpointManager.setFetchOffset(topicPartitionJobMap.get(tp), latestOffset);
            }
            break;
          case DO_NOT_SEEK:
            // do not seek means going to the committed offset.
            // although we don't need to seek, we still need to set the offsets.
            OffsetAndMetadata committedOffset = kafkaConsumer.committed(tp);
            if (committedOffset != null) {
              checkpointManager.setCommittedOffset(
                  topicPartitionJobMap.get(tp), committedOffset.offset());
              checkpointManager.setFetchOffset(
                  topicPartitionJobMap.get(tp), committedOffset.offset());
            } else {
              handleNoCommittedOffsetWhenDoNotSeekPolicy(
                  kafkaConsumer, checkpointManager, topicPartitionJobMap.get(tp), tp);
            }
            break;
          default:
            // do nothing
        }
        LOGGER.debug(
            "seek kafka consumer",
            StructuredLogging.kafkaGroup(
                pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getConsumerGroup()),
            StructuredLogging.kafkaTopic(tp.topic()),
            StructuredLogging.kafkaPartition(tp.partition()),
            StructuredLogging.kafkaOffset(offset));
      }
    }
  }

  private void logStartOffsetInfo(
      String group, String topic, int partition, long startOffset, String msg) {
    LOGGER.warn(
        msg,
        StructuredLogging.kafkaGroup(group),
        StructuredLogging.kafkaTopic(topic),
        StructuredLogging.kafkaPartition(partition),
        StructuredLogging.kafkaOffset(startOffset));
    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaGroup(group)
                .setKafkaTopic(topic)
                .setKafkaPartition(partition)
                .build())
        .counter(msg)
        .inc(1);
  }

  /**
   * Gets which offset the Kafka consumer should seek to. This is the offset to start to consume
   * messages.
   *
   * @param specifiedOffset the start offset specified by configurations.
   * @param earliestOffset the earliest available offset.
   * @param latestOffset the latest available offset.
   * @param autoOffsetResetPolicy the autoOffsetResetPolicy when the specifiedOffset is not
   *     negative, and is not in [earliestOffset, latestOffset]
   * @return a SeekStartOffsetOption indicating how to seek the start offset.
   */
  public abstract SeekStartOffsetOption getSeekStartOffsetOption(
      long specifiedOffset,
      @Nullable Long earliestOffset,
      @Nullable Long latestOffset,
      AutoOffsetResetPolicy autoOffsetResetPolicy);

  @Nullable
  ConsumerRecords<K, V> pollMessages(Map<TopicPartition, Job> allTopicPartitionJobMap)
      throws InterruptedException {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    @Nullable ConsumerRecords<K, V> records = null;
    if (!allTopicPartitionJobMap.isEmpty()) {
      allTopicPartitionJobMap.forEach(
          (tp, job) -> {
            LOGGER.debug(
                "kafka.poll",
                StructuredLogging.jobId(job.getJobId()),
                StructuredLogging.kafkaCluster(job.getKafkaConsumerTask().getCluster()),
                StructuredLogging.kafkaGroup(job.getKafkaConsumerTask().getConsumerGroup()),
                StructuredLogging.kafkaPartition(job.getKafkaConsumerTask().getPartition()));
          });
      Stopwatch kafkaPollTimer =
          scope
              .tagged(
                  StructuredTags.builder()
                      .setKafkaGroup(
                          pipelineStateManager
                              .getJobTemplate()
                              .getKafkaConsumerTask()
                              .getConsumerGroup())
                      .build())
              .timer(MetricNames.KAFKA_POLL_LATENCY)
              .start();
      try {
        records = kafkaConsumer.poll(java.time.Duration.ofMillis(pollTimeoutMs));
      } catch (KafkaException e) {
        scope
            .tagged(
                StructuredTags.builder()
                    .setKafkaCluster(
                        pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getCluster())
                    .setKafkaGroup(
                        pipelineStateManager
                            .getJobTemplate()
                            .getKafkaConsumerTask()
                            .getConsumerGroup())
                    .setKafkaTopic(
                        pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getTopic())
                    .build())
            .counter(MetricNames.KAFKA_POLL_EXCEPTION)
            .inc(1);
        throw e;
      } finally {
        kafkaPollTimer.stop();
      }
      LOGGER.debug(
          "kafka.poll.success", StructuredLogging.count(records == null ? 0 : records.count()));
    } else {
      LOGGER.debug("will not fetch messages", StructuredLogging.reason("there is no job assigned"));
      try {
        Thread.sleep(FETCHER_IDLE_SLEEP_MS);
      } catch (InterruptedException e) {
        LOGGER.info("Sleep was interrupted", e);
      }
      records = ConsumerRecords.empty();
    }
    return records;
  }

  @VisibleForTesting
  void updateActualRunningJobStatus() {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    ImmutableList.Builder<JobStatus> builder = new ImmutableList.Builder<>();
    currentRunningJobMap.forEach(
        (jobId, job) -> {
          JobStatus.Builder jobStatusBuilder = JobStatus.newBuilder();
          jobStatusBuilder.setJob(job);
          jobStatusBuilder.setState(JobState.JOB_STATE_RUNNING);
          CheckpointInfo checkpointInfo = checkpointManager.getCheckpointInfo(job);
          ThroughputTracker.Throughput throughput = throughputTracker.getThroughput(job);
          InflightMessageTracker.InflightMessageStats inflightMessageStats =
              inflightMessageTracker.getInflightMessageStats(
                  new TopicPartition(
                      job.getKafkaConsumerTask().getTopic(),
                      job.getKafkaConsumerTask().getPartition()));
          KafkaConsumerTaskStatus kafkaConsumerTaskStatus =
              KafkaConsumerTaskStatus.newBuilder()
                  .setReadOffset(checkpointInfo.getFetchOffset())
                  .setCommitOffset(checkpointInfo.getCommittedOffset())
                  .setMessagesPerSec(throughput.messagePerSec)
                  .setBytesPerSec(throughput.bytesPerSec)
                  .setTotalMessagesInflight(inflightMessageStats.numberOfMessages.get())
                  .setTotalBytesInflight(inflightMessageStats.totalBytes.get())
                  .build();
          jobStatusBuilder.setKafkaConsumerTaskStatus(kafkaConsumerTaskStatus);
          builder.add(jobStatusBuilder.build());
        });
    pipelineStateManager.updateActualRunningJobStatus(builder.build());
  }

  @VisibleForTesting
  void processFetchedData(
      @Nullable ConsumerRecords<K, V> consumerRecords, Map<TopicPartition, Job> taskMap)
      throws InterruptedException {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    Preconditions.checkNotNull(processorSink, "sink required");
    if (taskMap.size() == 0 || consumerRecords == null || consumerRecords.isEmpty()) {
      return;
    }

    Set<TopicPartition> partitions = consumerRecords.partitions();
    for (TopicPartition tp : partitions) {
      List<ConsumerRecord<K, V>> records = consumerRecords.records(tp);
      Job job = taskMap.get(tp);

      if (records.size() != 0) {
        // make sure the job is not null
        if (job == null) {
          pausePartitionAndSeekOffset(tp, records.get(0).offset());
          continue;
        }

        final int size = records.size();
        long lastProcessedOffset = records.get(size - 1).offset();
        for (int i = 0; i < size; ++i) {
          ConsumerRecord<K, V> record = records.get(i);
          // FIXME: this is a workaround for retry queue duplication problem.
          // https://t3.uberinternal.com/browse/KAFEP-1522
          // long term solution is to have a separate thread for periodic commit offset, however, it
          // is a complicated change.
          // add maybeCommitOffsets method in here to make offset commit more frequent so that fewer
          // messages get reprocessed when worker shuffle happened
          if (perRecordCommit
              && (lastCommitOffsetsCheckTimeMs == -1L
                  || (System.currentTimeMillis() - lastCommitOffsetsCheckTimeMs
                      > ACTIVE_COMMIT_INTERVAL_IN_MS))) {
            commitOffsets(taskMap);
          }
          if (handleEndOffsetAndDelay(record, job, checkpointManager, pipelineStateManager)) {
            lastProcessedOffset = record.offset() - 1;
            pausePartitionAndSeekOffset(tp, record.offset());
            break;
          }
          if (delayProcessManager.shouldDelayProcess(record)) {
            delayProcessManager.pausedPartitionsAndRecords(tp, records.subList(i, size));
            lastProcessedOffset = record.offset() - 1;
            break;
          }

          scope
              .tagged(
                  StructuredTags.builder()
                      .setKafkaGroup(
                          pipelineStateManager
                              .getJobTemplate()
                              .getKafkaConsumerTask()
                              .getConsumerGroup())
                      .setKafkaTopic(tp.topic())
                      .setKafkaPartition(tp.partition())
                      .build())
              .gauge(MetricNames.TOPIC_PARTITION_DELAY_TIME)
              .update(System.currentTimeMillis() - record.timestamp());

          inflightMessageTracker.addMessage(tp, record.serializedValueSize());
          // process the message
          final Scope scopeWithGroupTopicPartition =
              scope.tagged(
                  StructuredTags.builder()
                      .setKafkaGroup(job.getKafkaConsumerTask().getConsumerGroup())
                      .setKafkaTopic(record.topic())
                      .setKafkaPartition(record.partition())
                      .build());
          final Timer messageProcessTimer =
              scopeWithGroupTopicPartition.timer(MetricNames.MESSAGE_PROCESS_LATENCY);
          long startNanoTime = System.nanoTime();
          TracedConsumerRecord<K, V> tracedRecord =
              TracedConsumerRecord.of(
                  record, infra.tracer(), job.getKafkaConsumerTask().getConsumerGroup());
          processorSink
              .submit(ItemAndJob.of(tracedRecord, job))
              .toCompletableFuture()
              .whenComplete(
                  (aLong, throwable) -> {
                    messageProcessTimer.record(Duration.between(startNanoTime, System.nanoTime()));
                    if (throwable != null) {
                      // because the fetcher side does not have a retry mechanism, the processor
                      // should handle all errors and never return an error to the fetcher
                      LOGGER.error(
                          "failed to process a record",
                          StructuredLogging.jobId(job.getJobId()),
                          StructuredLogging.kafkaGroup(
                              job.getKafkaConsumerTask().getConsumerGroup()),
                          StructuredLogging.kafkaTopic(record.topic()),
                          StructuredLogging.kafkaPartition(record.partition()),
                          StructuredLogging.kafkaOffset(record.offset()),
                          throwable);
                      scopeWithGroupTopicPartition
                          .counter(MetricNames.MESSAGE_PROCESS_FAILURE)
                          .inc(1);
                    } else {
                      // this setOffsetToCommit will keep the largest offset to commit.
                      checkpointManager.setOffsetToCommit(job, aLong);
                      // update throughput
                      throughputTracker.record(job, 1, record.serializedValueSize());
                    }
                    inflightMessageTracker.removeMessage(tp, record.serializedValueSize());
                    tracedRecord.complete(aLong, throwable);
                  });
        }
        // set next fetch lastProcessedOffset after current records enqueue succeed. When fetch
        // request failed,
        // fetcher will reset to fetchOffset to avoid data loss.
        checkpointManager.setFetchOffset(job, lastProcessedOffset + 1);
      } else {
        if (job == null) {
          pausePartitionAndSeekOffset(tp, -1L);
        }
      }
    }
  }

  /**
   * Handles the case when the AutoOffsetResetPolicy = DO_NOT_SEEK but committed offset is not
   * available, this is mainly because the consumer group for the topic partition is newly created
   * and there is no consumption happened before
   *
   * @param kafkaConsumer The KafkaConsumer
   * @param checkpointManager The checkpointManager
   * @param job The job
   * @param tp The topic partition
   */
  public void handleNoCommittedOffsetWhenDoNotSeekPolicy(
      Consumer kafkaConsumer, CheckpointManager checkpointManager, Job job, TopicPartition tp) {}

  /**
   * Pre-process the ConsumerRecord before sending to the next stage for processing.
   *
   * <p>Typically, it does two things
   *
   * <ol>
   *   <li>blocks some time until the ConsumerRecord can be processed.
   *   <li>decides whether the caller needs to process the remaining messages for the given job or
   *       not.
   * </ol>
   *
   * @param consumerRecord the ConsumerRecord to process.
   * @param job which job the ConsumerRecord belongs to.
   * @param checkpointManager the CheckPointManager holding the up-to-date check point information.
   * @param pipelineStateManager the PipelineStateManager holding the up-to-date configurations and
   *     actual consuming state.
   * @return a boolean indicating whether the caller needs to process the remaining messages for the
   *     given job or not. True means the caller does not need to, false means otherwise.
   * @throws InterruptedException if the process is interrupted.
   */
  public abstract boolean handleEndOffsetAndDelay(
      ConsumerRecord<K, V> consumerRecord,
      Job job,
      CheckpointManager checkpointManager,
      PipelineStateManager pipelineStateManager)
      throws InterruptedException;

  private void pausePartitionAndSeekOffset(TopicPartition tp, long offset) {
    Preconditions.checkNotNull(pipelineStateManager, "pipeline config manager required");
    kafkaConsumer.pause(Collections.singleton(tp));
    if (offset >= 0) {
      kafkaConsumer.seek(tp, offset);
    }
    LOGGER.warn(
        "kafka.pause",
        StructuredLogging.kafkaGroup(
            pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getConsumerGroup()),
        StructuredLogging.kafkaTopic(tp.topic()),
        StructuredLogging.kafkaPartition(tp.partition()),
        StructuredLogging.reason("there is no job for this topic-partition"));
    scope
        .tagged(
            StructuredTags.builder()
                .setKafkaGroup(
                    pipelineStateManager.getJobTemplate().getKafkaConsumerTask().getConsumerGroup())
                .setKafkaTopic(tp.topic())
                .setKafkaPartition(tp.partition())
                .build())
        .gauge(MetricNames.TOPIC_PARTITION_PAUSED)
        .update(1);
  }

  @VisibleForTesting
  void logTopicPartitionOffsetInfo(Map<TopicPartition, Job> taskMap) {
    if ((System.currentTimeMillis() - lastDumpTime) < offsetMonitorMs
        || (taskMap.size() == 0 && kafkaConsumer.assignment().size() == 0)) {
      return;
    }
    ImmutableMap.Builder<TopicPartition, Long> topicPartitionOffsetMap = ImmutableMap.builder();
    taskMap.forEach(
        (key, value) ->
            topicPartitionOffsetMap.put(
                key, checkpointManager.getCheckpointInfo(value).getFetchOffset()));
    LOGGER.debug(
        "fetching from Kafka",
        StructuredLogging.topicPartitionOffsets(topicPartitionOffsetMap.build()));
    reportKafkaConsumerMetrics();
    lastDumpTime = System.currentTimeMillis();
  }

  @VisibleForTesting
  void reportKafkaConsumerMetrics() {
    Collection<? extends Metric> metrics = ImmutableList.of();
    metrics = kafkaConsumer.metrics().values();

    for (Metric metric : metrics) {
      // KafkaConsumer.metrics().values() returns an untyped Object.
      // In 2.2.1, the Object can be a generic type T or a Double:
      // kafka/clients/src/main/java/org/apache/kafka/common/metrics/KafkaMetric.java#L65
      // kafka/clients/src/main/java/org/apache/kafka/common/metrics/Measurable.java#L30
      // We report only the Double metrics since we have no way of determining the type parameter T.
      if (metric.metricValue() instanceof Double) {
        scope
            .tagged(metric.metricName().tags())
            .gauge(metric.metricName().name())
            .update((Double) metric.metricValue());
        LOGGER.debug(
            "emit KafkaConsumer metric", StructuredLogging.metricName(metric.metricName().name()));
      } else {
        LOGGER.debug(
            "skipping KafkaConsumer metric",
            StructuredLogging.reason("type not Double"),
            StructuredLogging.metricName(metric.metricName().name()));
      }
    }
  }

  /**
   * Signal interrupts the thread that might be blocking so that it refreshes updates from pipeline
   * manager.
   *
   * <p>The default implementation doesn't signal anything.
   */
  public CompletionStage<Void> signal() {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isRunning() {
    return super.isRunning();
  }

  public void close() {
    // After successfully shutdown, doWork will not be scheduled again.
    // Stopping doWork before the following code helps solve the
    // java.util.ConcurrentModificationException caused by invoking different methods of the
    // kafka consumer from different threads.
    try {
      shutdown();
    } catch (InterruptedException e) {
      log.warn("Interrupted while shutting down KafkaFetcher", e);
    }
    cleanup();
  }

  private void cleanup() {
    try {
      // commit offsets for the last time
      Map<TopicPartition, Job> addedTopicPartitionJobMap = new HashMap<>();
      Map<TopicPartition, Job> removedTopicPartitionJobMap = new HashMap<>();
      Map<TopicPartition, Job> allTopicPartitionJobMap = new HashMap<>();
      // the extractTopicPartitionMap method will modify three passed-in maps, therefore, do not
      // make those maps immutable.
      extractTopicPartitionMap(
          addedTopicPartitionJobMap, removedTopicPartitionJobMap, allTopicPartitionJobMap);
      commitOffsets(allTopicPartitionJobMap);
      // aync commit needs another poll
      Instrumentation.instrument.returnVoidCatchThrowable(
          LOGGER,
          scope,
          () -> kafkaConsumer.poll(java.time.Duration.ZERO),
          "fetcher.kafka.last-poll");

      // wake up and close kafka consumer
      kafkaConsumer.wakeup();
      kafkaConsumer.close();
      checkpointManager.close();
      delayProcessManager.close();
      throughputTracker.clear();
      inflightMessageTracker.clear();
      if (!scheduledExecutorService.isShutdown()) {
        scheduledExecutorService.shutdown();
      }
      scope.counter(MetricNames.CLOSE_SUCCESS).inc(1);
    } catch (Exception e) {
      LOGGER.error(MetricNames.CLOSE_FAILURE, e);
      scope.counter(MetricNames.CLOSE_FAILURE).inc(1);
      throw new RuntimeException(e);
    }
  }

  private static class MetricNames {

    static final String FETCHER_KAFKA_WORKING = "fetcher.kafka.working";
    static final String FETCHER_KAFKA_NOT_WORKING = "fetcher.kafka.not.working";
    static final String OFFSET_COMMIT_EXCEPTION = "fetcher.kafka.offset.commit.exception";
    static final String TOPIC_PARTITION_OWNED_EXPECTED =
        "fetcher.kafka.topic.partition.owned.expected";
    static final String TOPIC_PARTITION_OWNED_ACTUAL = "fetcher.kafka.topic.partition.owned.actual";
    static final String MESSAGE_PROCESS_FAILURE = "fetcher.kafka.message.process.failure";
    static final String MESSAGE_PROCESS_LATENCY = "fetcher.kafka.message.process.latency";
    static final String TOPIC_PARTITION_SEEK_TOO_SMALL =
        "fetcher.kafka.topic.partition.seek.too-small";
    static final String TOPIC_PARTITION_SEEK_TOO_LARGE =
        "fetcher.kafka.topic.partition.seek.too-large";
    static final String TOPIC_PARTITION_SEEK_START_NULL =
        "fetcher.kafka.topic.partition.seek.start-null";
    static final String TOPIC_PARTITION_SEEK_END_NULL =
        "fetcher.kafka.topic.partition.seek.end-null";
    static final String TOPIC_PARTITION_DUP_JOB = "fetcher.kafka.topic.partition.dup-job";
    static final String TOPIC_PARTITION_PAUSED = "fetcher.kafka.topic.partition.paused";
    static final String TOPIC_PARTITION_RESUME = "fetcher.kafka.topic.partition.resume";
    static final String TOPIC_PARTITION_DELAY_TIME = "fetcher.kafka.topic.partition.delay.time";
    static final String TOPIC_PARTITION_OFFSET_START = "fetcher.kafka.topic.partition.offset.start";
    static final String TOPIC_PARTITION_DELAY_DUPLICATION =
        "fetcher.kafka.topic.partition.delay.duplication";
    static final String TOPIC_PARTITION_OFFSET_END = "fetcher.kafka.topic.partition.offset.end";
    static final String CLOSE_SUCCESS = "fetcher.kafka.close.success";
    static final String CLOSE_FAILURE = "fetcher.kafka.close.failure";
    static final String KAFKA_POLL_LATENCY = "fetcher.kafka.poll.latency";
    static final String KAFKA_POLL_EXCEPTION = "fetcher.kafka.poll.exception";
    static final String OFFSET_FETCH_LATENCY = "fetcher.kafka.offset.fetch.latency";
    static final String OFFSET_FETCH_EXCEPTION = "fetcher.kafka.offset.fetch.exception";
    static final String OFFSET_COMMIT_LATENCY = "fetcher.kafka.offset.commit.latency";
    static final String OFFSET = "fetcher.kafka.offset";
    static final String OFFSET_COMMIT_SUCCESS = "fetcher.kafka.offset.commit.success";
    static final String OFFSET_COMMIT_FAILURE = "fetcher.kafka.offset.commit.failure";
  }
}
