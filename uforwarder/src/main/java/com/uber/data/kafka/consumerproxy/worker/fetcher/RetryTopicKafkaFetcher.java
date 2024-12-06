package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.AbstractKafkaFetcherThread;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.CheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.DelayProcessManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.InflightMessageTracker;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaCheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaDelayProcessManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcherConfiguration;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.SeekStartOffsetOption;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.ThroughputTracker;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RetryTopicKafkaFetcher is for endless Kafka message fetching and processing from the retry topic,
 * with potential message processing delay. It handles {@link
 * com.uber.data.kafka.datatransfer.KafkaConsumerConfiguration} in the following way:
 *
 * <ol>
 *   <li>It only deals with PROCESSING_DELAY_MS.
 *   <li>It handle each message no earlier than "message TS + PROCESSING_DELAY_MS".
 * </ol>
 */
public final class RetryTopicKafkaFetcher extends AbstractKafkaFetcherThread<byte[], byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetryTopicKafkaFetcher.class);

  private final CoreInfra infra;
  // the lock for the Condition
  private final ReentrantLock lock;
  // Condition for processing delay
  private final Condition processingDelayReached;

  private final Optional<RetryQueue> retryQueueConfig;

  RetryTopicKafkaFetcher(
      String threadName,
      KafkaFetcherConfiguration config,
      CheckpointManager checkpointManager,
      ThroughputTracker throughputTracker,
      DelayProcessManager<byte[], byte[]> delayProcessManager,
      Consumer<byte[], byte[]> kafkaConsumer,
      CoreInfra infra,
      Optional<RetryQueue> retryQueueConfig) {
    super(
        threadName,
        config,
        checkpointManager,
        throughputTracker,
        delayProcessManager,
        new InflightMessageTracker(),
        kafkaConsumer,
        infra,
        false,
        // use hardcoded value since this is a temporary workaround.
        true);
    this.infra = infra;
    this.lock = new ReentrantLock(false);
    this.processingDelayReached = this.lock.newCondition();
    this.retryQueueConfig = retryQueueConfig;
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
  @Override
  public SeekStartOffsetOption getSeekStartOffsetOption(
      long specifiedOffset,
      @Nullable Long earliestOffset,
      @Nullable Long latestOffset,
      AutoOffsetResetPolicy autoOffsetResetPolicy) {
    // we don't need to see as for the retry topic, we want to start from
    // 1. the last committed offset if it exists.
    // 2. the earliest/latest offset if the last committed offset does not exist. This is based on
    // ConsumerConfig.AUTO_OFFSET_RESET_POLICY setted in KafkaFetcherConfiguration
    return SeekStartOffsetOption.DO_NOT_SEEK;
  }

  /**
   * Pre-process the ConsumerRecord before sending to the next stage (for example, a {@link
   * com.uber.data.kafka.datatransfer.worker.processors.Processor}) for processing.
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
  @Override
  public boolean handleEndOffsetAndDelay(
      ConsumerRecord<byte[], byte[]> consumerRecord,
      Job job,
      CheckpointManager checkpointManager,
      PipelineStateManager pipelineStateManager)
      throws InterruptedException {
    // If the job is no longer assigned to this pipeline, there
    // is no need to process the remaining records.
    // The delay processing is handled by the delayProcessManager.
    return !pipelineStateManager.shouldJobBeRunning(job);
  }

  @Override
  public CompletionStage<Void> signal() {
    lock.lock();
    try {
      // wake up the waiting in case the job is not expected to be running any more.
      processingDelayReached.signalAll();
    } finally {
      lock.unlock();
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Creates a RetryTopicKafkaFetcher with supplied parameters
   *
   * @param threadName the name for the RQ fetcher thread
   * @param bootstrapServer a list of bootstrap servers
   * @param consumerGroup the consumer group name
   * @param config the config for the fetcher
   * @param retryQueueConfig the config for retry queue
   * @param isSecure determines if it's secure or not
   * @param infra the infra
   * @return the constructed RetryTopicKafkaFetcher
   */
  public static RetryTopicKafkaFetcher of(
      String threadName,
      String bootstrapServer,
      String consumerGroup,
      AutoOffsetResetPolicy autoOffsetResetPolicy,
      KafkaFetcherConfiguration config,
      Optional<RetryQueue> retryQueueConfig,
      boolean isSecure,
      CoreInfra infra) {
    KafkaConsumer<byte[], byte[]> kafkaConsumer =
        new KafkaConsumer<>(
            config.getKafkaConsumerProperties(
                bootstrapServer,
                threadName,
                consumerGroup,
                autoOffsetResetPolicy,
                IsolationLevel.ISOLATION_LEVEL_UNSET,
                isSecure));
    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(infra.scope());
    ThroughputTracker throughputTracker = new ThroughputTracker();
    int processingDelayMs = -1;
    if (retryQueueConfig != null && retryQueueConfig.isPresent()) {
      processingDelayMs = retryQueueConfig.get().getProcessingDelayMs();
    }
    DelayProcessManager<byte[], byte[]> delayProcessManager =
        new KafkaDelayProcessManager<>(
            infra.scope(), consumerGroup, processingDelayMs, kafkaConsumer);
    return new RetryTopicKafkaFetcher(
        threadName,
        config,
        checkpointManager,
        throughputTracker,
        delayProcessManager,
        kafkaConsumer,
        infra,
        retryQueueConfig);
  }
}
