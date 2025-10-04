package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.AutoOffsetResetPolicy;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.PipelineStateManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class KafkaFetcherThreadTest {
  AbstractKafkaFetcherThread fetcherThread;

  @BeforeEach
  public void setUp() {
    CoreInfra infra = CoreInfra.NOOP;
    fetcherThread =
        new KafkaFetcherThread(
            "THREAD_NAME",
            new KafkaFetcherConfiguration(),
            new KafkaCheckpointManager(infra.scope()),
            new ThroughputTracker(),
            Mockito.mock(Consumer.class),
            infra);
  }

  @Test
  public void testGetSeekStartOffsetOption() {
    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));

    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));

    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            7, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            12, 5L, 10L, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));

    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, null, null, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_INVALID));
    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, null, null, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_EARLIEST));
    Assertions.assertEquals(
        SeekStartOffsetOption.DO_NOT_SEEK,
        fetcherThread.getSeekStartOffsetOption(
            1, null, null, AutoOffsetResetPolicy.AUTO_OFFSET_RESET_POLICY_LATEST));
  }

  @Test
  public void testHandleEndOffsetAndDelay() throws InterruptedException {
    Assertions.assertFalse(
        fetcherThread.handleEndOffsetAndDelay(
            Mockito.mock(ConsumerRecord.class),
            Job.getDefaultInstance(),
            Mockito.mock(CheckpointManager.class),
            Mockito.mock(PipelineStateManager.class)));
  }
}
