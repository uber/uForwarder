package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class KafkaCheckpointManagerTest extends FievelTestBase {

  private Job job;
  private KafkaCheckpointManager kafkaCheckpointManager;

  @Before
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
    Assert.assertEquals(job, checkpointInfo.getJob());
    Assert.assertEquals(-1, checkpointInfo.getStartingOffset());
    Assert.assertEquals(-1, checkpointInfo.getFetchOffset());
    Assert.assertEquals(-1, checkpointInfo.getOffsetToCommit());
  }

  @Test
  public void testAddCheckpointInfo() {
    CheckpointInfo checkpointInfo = kafkaCheckpointManager.addCheckpointInfo(job);
    Assert.assertEquals(job, checkpointInfo.getJob());
    Assert.assertEquals(-1, checkpointInfo.getStartingOffset());
    Assert.assertEquals(-1, checkpointInfo.getFetchOffset());
    Assert.assertEquals(-1, checkpointInfo.getOffsetToCommit());
  }

  @Test
  public void testSet() {
    kafkaCheckpointManager.setOffsetToCommit(job, 60);
    kafkaCheckpointManager.setFetchOffset(job, 70);
    kafkaCheckpointManager.setCommittedOffset(job, 80);
    CheckpointInfo checkpointInfo = kafkaCheckpointManager.getCheckpointInfo(job);
    Assert.assertEquals(job, checkpointInfo.getJob());
    Assert.assertEquals(-1L, checkpointInfo.getStartingOffset());
    Assert.assertEquals(70L, checkpointInfo.getFetchOffset());
    Assert.assertEquals(60L, checkpointInfo.getOffsetToCommit());
    Assert.assertEquals(80L, checkpointInfo.getCommittedOffset());
  }

  @Test
  public void testGetOffsetToCommit() {
    Assert.assertEquals(
        KafkaUtils.MAX_INVALID_OFFSET_TO_COMMIT, kafkaCheckpointManager.getOffsetToCommit(job));

    kafkaCheckpointManager.setOffsetToCommit(job, 60L);
    kafkaCheckpointManager.setCommittedOffset(job, 60L);
    Assert.assertEquals(60, kafkaCheckpointManager.getOffsetToCommit(job));

    kafkaCheckpointManager.setOffsetToCommit(job, 70L);
    Assert.assertNotEquals(
        KafkaUtils.MAX_INVALID_OFFSET_TO_COMMIT, kafkaCheckpointManager.getOffsetToCommit(job));

    kafkaCheckpointManager.setOffsetToCommit(job, 59L);
    Assert.assertNotEquals(
        KafkaUtils.MAX_INVALID_OFFSET_TO_COMMIT, kafkaCheckpointManager.getOffsetToCommit(job));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonKafkaConsumerTask() {
    Job job = Job.newBuilder().build();
    kafkaCheckpointManager.setFetchOffset(job, 10L);
  }

  @Test
  public void testClose() {
    kafkaCheckpointManager.close();
  }
}
