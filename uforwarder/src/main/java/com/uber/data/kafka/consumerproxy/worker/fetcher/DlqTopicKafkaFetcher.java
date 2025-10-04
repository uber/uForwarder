package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.AbstractKafkaFetcherThread;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.CheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.DelayProcessManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaCheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcherConfiguration;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.SeekStartOffsetOption;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.ThroughputTracker;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DlqTopicKafkaFetcher is for Kafka message fetching and processing from the dlq topic. It handles
 * {@link com.uber.data.kafka.datatransfer.KafkaConsumerConfiguration} in the following way:
 *
 * <ol>
 *   <li>It ignores AUTO_OFFSET_RESET_POLICY and PROCESSING_DELAY_MS.
 *   <li>It processes messages starting from an existing offset that's closest to the START_OFFSET,
 *       and ending at END_OFFSET.
 * </ol>
 */
public final class DlqTopicKafkaFetcher extends AbstractKafkaFetcherThread<byte[], byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DlqTopicKafkaFetcher.class);

  DlqTopicKafkaFetcher(
      String threadName,
      KafkaFetcherConfiguration config,
      CheckpointManager checkpointManager,
      ThroughputTracker throughputTracker,
      Consumer<byte[], byte[]> kafkaConsumer,
      CoreInfra infra) {
    super(
        threadName,
        config,
        checkpointManager,
        throughputTracker,
        DelayProcessManager.NOOP,
        kafkaConsumer,
        infra,
        false,
        true);
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
    if (earliestOffset != null && earliestOffset > specifiedOffset) {
      return SeekStartOffsetOption.SEEK_TO_EARLIEST_OFFSET;
    }
    // treat endOffset == null as not offset of range
    if (latestOffset != null && latestOffset < specifiedOffset) {
      return SeekStartOffsetOption.SEEK_TO_LATEST_OFFSET;
    }
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
      PipelineStateManager pipelineStateManager) {
    // if the current offset is bounded by the end offset, there
    // is no need to process the remaining records.
    return checkpointManager.getCheckpointInfo(job).bounded(consumerRecord.offset())
        ||
        // If the job is a purge job, we still won't process the messages
        // even if they are within the range
        job.getKafkaConsumerTask().getStartOffset() == job.getKafkaConsumerTask().getEndOffset()
        ||
        // If the job is no longer assigned to this pipeline, there
        // is no need to process the remaining records.
        !pipelineStateManager.shouldJobBeRunning(job);
  }

  public static DlqTopicKafkaFetcher of(
      String threadName,
      String bootstrapServers,
      String consumerGroup,
      KafkaFetcherConfiguration config,
      boolean isSecure,
      CoreInfra infra)
      throws Exception {
    KafkaConsumer<byte[], byte[]> kafkaConsumer =
        new KafkaConsumer<>(
            config.getKafkaConsumerProperties(
                bootstrapServers,
                threadName,
                consumerGroup,
                IsolationLevel.ISOLATION_LEVEL_UNSET,
                isSecure));
    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(infra.scope());
    ThroughputTracker throughputTracker = new ThroughputTracker();
    return new DlqTopicKafkaFetcher(
        threadName, config, checkpointManager, throughputTracker, kafkaConsumer, infra);
  }
}
