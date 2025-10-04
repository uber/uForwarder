package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class KafkaCheckpointManagerTest {

  private Job job;
  private KafkaCheckpointManager kafkaCheckpointManager;

  @BeforeEach
  public void setUp() throws Exception {
    job =
        Job.newBuilder()
            .setJobId(1)
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setTopic("topic")
                    .setCluster("cluster")
                    .setConsumerGroup("group")
                    .setPartition(1)
                    .setStartOffset(-1)
                    .setEndOffset(100)
                    .build())
            .build();
    KafkaFetcherConfiguration config = new KafkaFetcherConfiguration();
    Scope scope = Mockito.mock(Scope.class);
    Counter counter = Mockito.mock(Counter.class);
    Timer timer = Mockito.mock(Timer.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Stopwatch stopwatch = Mockito.mock(Stopwatch.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    Mockito.when(timer.start()).thenReturn(stopwatch);
    kafkaCheckpointManager = new KafkaCheckpointManager(scope);
  }

  @Test
  public void testGetCheckpointInfo() {
    CheckpointInfo checkpointInfo = kafkaCheckpointManager.getCheckpointInfo(job);
    Assertions.assertEquals(job, checkpointInfo.getJob());
    Assertions.assertEquals(-1, checkpointInfo.getStartingOffset());
    Assertions.assertEquals(-1, checkpointInfo.getFetchOffset());
    Assertions.assertEquals(-1, checkpointInfo.getOffsetToCommit());
  }

  @Test
  public void testAddCheckpointInfo() {
    CheckpointInfo checkpointInfo = kafkaCheckpointManager.addCheckpointInfo(job);
    Assertions.assertEquals(job, checkpointInfo.getJob());
    Assertions.assertEquals(-1, checkpointInfo.getStartingOffset());
    Assertions.assertEquals(-1, checkpointInfo.getFetchOffset());
    Assertions.assertEquals(-1, checkpointInfo.getOffsetToCommit());
  }

  @Test
  public void testSet() {
    kafkaCheckpointManager.setOffsetToCommit(job, 60);
    kafkaCheckpointManager.setFetchOffset(job, 70);
    kafkaCheckpointManager.setCommittedOffset(job, 80);
    CheckpointInfo checkpointInfo = kafkaCheckpointManager.getCheckpointInfo(job);
    Assertions.assertEquals(job, checkpointInfo.getJob());
    Assertions.assertEquals(-1L, checkpointInfo.getStartingOffset());
    Assertions.assertEquals(70L, checkpointInfo.getFetchOffset());
    Assertions.assertEquals(60L, checkpointInfo.getOffsetToCommit());
    Assertions.assertEquals(80L, checkpointInfo.getCommittedOffset());
  }

  @Test
  public void testGetOffsetToCommit() {
    Assertions.assertEquals(
        KafkaUtils.MAX_INVALID_OFFSET_TO_COMMIT, kafkaCheckpointManager.getOffsetToCommit(job));

    kafkaCheckpointManager.setOffsetToCommit(job, 60L);
    kafkaCheckpointManager.setCommittedOffset(job, 60L);
    Assertions.assertEquals(60, kafkaCheckpointManager.getOffsetToCommit(job));

    kafkaCheckpointManager.setOffsetToCommit(job, 70L);
    Assertions.assertNotEquals(
        KafkaUtils.MAX_INVALID_OFFSET_TO_COMMIT, kafkaCheckpointManager.getOffsetToCommit(job));

    kafkaCheckpointManager.setOffsetToCommit(job, 59L);
    Assertions.assertNotEquals(
        KafkaUtils.MAX_INVALID_OFFSET_TO_COMMIT, kafkaCheckpointManager.getOffsetToCommit(job));
  }

  @Test
  public void testNonKafkaConsumerTask() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          Job job = Job.newBuilder().build();
          kafkaCheckpointManager.setFetchOffset(job, 10L);
        });
  }

  @Test
  public void testClose() {
    kafkaCheckpointManager.close();
  }
}
