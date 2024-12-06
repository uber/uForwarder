package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * KafkaFetcherThread is the default implementation of {@link AbstractKafkaFetcherThread}, which
 * does not handle any field of {@link com.uber.data.kafka.datatransfer.KafkaConsumerConfiguration}.
 */
public final class KafkaFetcherThread extends AbstractKafkaFetcherThread<byte[], byte[]> {
  KafkaFetcherThread(
      String threadName,
      KafkaFetcherConfiguration config,
      CheckpointManager checkpointManager,
      ThroughputTracker throughputTracker,
      InflightMessageTracker inflightMessageTracker,
      Consumer<byte[], byte[]> kafkaConsumer,
      CoreInfra infra) {
    this(
        threadName,
        config,
        checkpointManager,
        throughputTracker,
        inflightMessageTracker,
        kafkaConsumer,
        infra,
        true);
  }

  KafkaFetcherThread(
      String threadName,
      KafkaFetcherConfiguration config,
      CheckpointManager checkpointManager,
      ThroughputTracker throughputTracker,
      InflightMessageTracker inflightMessageTracker,
      Consumer<byte[], byte[]> kafkaConsumer,
      CoreInfra infra,
      boolean asyncCommitOffset) {
    super(
        threadName,
        config,
        checkpointManager,
        throughputTracker,
        inflightMessageTracker,
        kafkaConsumer,
        infra,
        asyncCommitOffset);
  }

  KafkaFetcherThread(
      String threadName,
      KafkaFetcherConfiguration config,
      CheckpointManager checkpointManager,
      ThroughputTracker throughputTracker,
      DelayProcessManager delayProcessManager,
      InflightMessageTracker inflightMessageTracker,
      Consumer<byte[], byte[]> kafkaConsumer,
      CoreInfra infra,
      boolean asyncCommitOffset,
      boolean perRecordCommit) {
    super(
        threadName,
        config,
        checkpointManager,
        throughputTracker,
        delayProcessManager,
        inflightMessageTracker,
        kafkaConsumer,
        infra,
        asyncCommitOffset,
        perRecordCommit);
  }

  /**
   * Decides which offset the Kafka consumer should seek to. This is the offset to start to consume
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
    return false;
  }
}
