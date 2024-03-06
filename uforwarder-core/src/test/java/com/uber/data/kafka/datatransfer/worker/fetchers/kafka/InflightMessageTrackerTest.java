package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.fievel.testing.base.FievelTestBase;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InflightMessageTrackerTest extends FievelTestBase {
  private InflightMessageTracker inflightMessageTracker;

  private Job job;

  @Before
  public void setup() throws Exception {
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
    inflightMessageTracker = new InflightMessageTracker();
  }

  @Test
  public void testInflightMessageTracker() throws Exception {
    TopicPartition tp =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    inflightMessageTracker.init(
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition()));

    // test add message
    inflightMessageTracker.addMessage(tp, 100);
    Assert.assertEquals(
        1, inflightMessageTracker.getInflightMessageStats(tp).numberOfMessages.get(), 0.00001);
    Assert.assertEquals(
        100,
        inflightMessageTracker
            .getInflightMessageStats(
                new TopicPartition(
                    job.getKafkaConsumerTask().getTopic(),
                    job.getKafkaConsumerTask().getPartition()))
            .totalBytes
            .get(),
        0.00001);
    inflightMessageTracker.removeMessage(tp, 100);

    // test remove message
    Assert.assertEquals(
        0,
        inflightMessageTracker
            .getInflightMessageStats(
                new TopicPartition(
                    job.getKafkaConsumerTask().getTopic(),
                    job.getKafkaConsumerTask().getPartition()))
            .numberOfMessages
            .get(),
        0.00001);
    Assert.assertEquals(
        0,
        inflightMessageTracker
            .getInflightMessageStats(
                new TopicPartition(
                    job.getKafkaConsumerTask().getTopic(),
                    job.getKafkaConsumerTask().getPartition()))
            .totalBytes
            .get(),
        0.00001);

    // test revoke job
    inflightMessageTracker.revokeInflightStatsForJob(tp);
    Assert.assertEquals(
        inflightMessageTracker
            .getInflightMessageStats(
                new TopicPartition(
                    job.getKafkaConsumerTask().getTopic(),
                    job.getKafkaConsumerTask().getPartition()))
            .numberOfMessages
            .get(),
        0,
        0.00001);
    Assert.assertEquals(
        inflightMessageTracker
            .getInflightMessageStats(
                new TopicPartition(
                    job.getKafkaConsumerTask().getTopic(),
                    job.getKafkaConsumerTask().getPartition()))
            .totalBytes
            .get(),
        0,
        0.00001);

    // test clear
    inflightMessageTracker.init(tp);
    inflightMessageTracker.addMessage(tp, 100);
    inflightMessageTracker.clear();
    Assert.assertEquals(
        inflightMessageTracker
            .getInflightMessageStats(
                new TopicPartition(
                    job.getKafkaConsumerTask().getTopic(),
                    job.getKafkaConsumerTask().getPartition()))
            .numberOfMessages
            .get(),
        0,
        0.00001);
    Assert.assertEquals(
        inflightMessageTracker
            .getInflightMessageStats(
                new TopicPartition(
                    job.getKafkaConsumerTask().getTopic(),
                    job.getKafkaConsumerTask().getPartition()))
            .totalBytes
            .get(),
        0,
        0.00001);
  }
}
