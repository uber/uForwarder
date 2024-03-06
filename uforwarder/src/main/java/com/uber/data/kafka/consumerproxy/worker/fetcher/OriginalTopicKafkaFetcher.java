package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.IsolationLevel;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.AbstractKafkaFetcherThread;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.CheckpointManager;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.InflightMessageTracker;
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
 * OriginalTopicKafkaFetcher is for endless Kafka message fetching and processing from the origin
 * topic. It handles {@link com.uber.data.kafka.datatransfer.KafkaConsumerConfiguration} in the
 * following way:
 *
 * <ol>
 *   <li>it ignores PROCESSING_DELAY_MS and END_OFFSET
 *   <li>When START_OFFSET is negative, it processes messages starting from the last committed
 *       offset if it exists, otherwise, it starts from the earliest available offset. When
 *       START_OFFSET is not negative, it processes messages starting from START_OFFSET if it
 *       exists, otherwise, it uses AUTO_OFFSET_RESET_POLICY to decide whether to start from the
 *       earliest or latest available offset.
 * </ol>
 */
public final class OriginalTopicKafkaFetcher extends AbstractKafkaFetcherThread<byte[], byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OriginalTopicKafkaFetcher.class);

  OriginalTopicKafkaFetcher(
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
        new InflightMessageTracker(),
        kafkaConsumer,
        infra,
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
    boolean offsetOutOfRange = false;
    // treat startOffset == null and endOffset == null as not offset of range
    if ((earliestOffset != null && earliestOffset > specifiedOffset)
        || (latestOffset != null && latestOffset < specifiedOffset)) {
      offsetOutOfRange = true;
    }
    if (offsetOutOfRange) {
      switch (autoOffsetResetPolicy) {
        case AUTO_OFFSET_RESET_POLICY_EARLIEST:
          return SeekStartOffsetOption.SEEK_TO_EARLIEST_OFFSET;
        case AUTO_OFFSET_RESET_POLICY_LATEST:
          return SeekStartOffsetOption.SEEK_TO_LATEST_OFFSET;
        default:
          return SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET;
      }
    } else {
      // consumer honors "auto.offset.reset" when offset out of range
      //
      // "auto.offset.reset" is the configuration used to create a Kafka consumer. It is set by the
      // system manager.
      // SeekStartOffsetOption is different, it's the runtime configuration and it is set by users.
      // It can different from  "auto.offset.reset", so we need to handle it differently.
      return SeekStartOffsetOption.SEEK_TO_SPECIFIED_OFFSET;
    }
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
    // If the job is no longer assigned to this pipeline, there
    // is no need to process the remaining records.
    return !pipelineStateManager.shouldJobBeRunning(job);
  }

  public static OriginalTopicKafkaFetcher of(
      String threadName,
      String bootstrapServers,
      String consumerGroup,
      AutoOffsetResetPolicy autoOffsetResetPolicy,
      IsolationLevel isolationLevel,
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
                autoOffsetResetPolicy,
                isolationLevel,
                isSecure));
    KafkaCheckpointManager checkpointManager = new KafkaCheckpointManager(infra.scope());
    ThroughputTracker throughputTracker = new ThroughputTracker();
    return new OriginalTopicKafkaFetcher(
        threadName, config, checkpointManager, throughputTracker, kafkaConsumer, infra);
  }
}
