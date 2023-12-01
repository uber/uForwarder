package com.uber.data.kafka.consumerproxy.utils;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RetryUtilsTest extends FievelTestBase {

  private Job job = null;
  private RetryConfig retryConfig = null;
  private RetryQueue queue1 = null;
  private RetryQueue queue2 = null;
  private RetryQueue queue3 = null;

  @Before
  public void setup() {
    queue1 =
        RetryQueue.newBuilder()
            .setRetryQueueTopic("topic1")
            .setRetryCluster("cluster1")
            .setMaxRetryCount(5)
            .setProcessingDelayMs(10000)
            .build();
    queue2 =
        RetryQueue.newBuilder()
            .setRetryQueueTopic("topic2")
            .setRetryCluster("cluster1")
            .setMaxRetryCount(5)
            .setProcessingDelayMs(20000)
            .build();

    queue3 =
        RetryQueue.newBuilder()
            .setRetryQueueTopic("topic3")
            .setRetryCluster("cluster1")
            .setMaxRetryCount(5)
            .setProcessingDelayMs(30000)
            .build();

    retryConfig =
        RetryConfig.newBuilder()
            .addRetryQueues(queue1)
            .addRetryQueues(queue2)
            .setRetryEnabled(true)
            .build();
    job =
        Job.newBuilder()
            .setRetryConfig(retryConfig)
            .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().setDlqTopic("dlqtopic").build())
            .build();
  }

  @Test
  public void testGettingRetryTopicBasedOnCounts() {
    // Case 1
    Assert.assertEquals("topic1", RetryUtils.getKafkaDestinationRetryTopic(job, 4));

    // Case 2
    Assert.assertEquals("topic2", RetryUtils.getKafkaDestinationRetryTopic(job, 10));

    // Case 3
    Assert.assertEquals("dlqtopic", RetryUtils.getKafkaDestinationRetryTopic(job, 15));

    // Case 4: when retryConfig is empty
    String topic =
        RetryUtils.getKafkaDestinationRetryTopic(
            Job.newBuilder(job).setRetryConfig(RetryConfig.newBuilder().build()).build(), 3);
    Assert.assertEquals("dlqtopic", topic);

    // Case 4: when retry queues are not enabled
    topic =
        RetryUtils.getKafkaDestinationRetryTopic(
            Job.newBuilder(job)
                .setRetryConfig(RetryConfig.newBuilder(retryConfig).setRetryEnabled(false).build())
                .build(),
            3);
    Assert.assertEquals("dlqtopic", topic);

    // Case 5: when DLQ is empty
    topic =
        RetryUtils.getKafkaDestinationRetryTopic(
            Job.newBuilder(job)
                .setRpcDispatcherTask(RpcDispatcherTask.newBuilder().build())
                .build(),
            15);
    Assert.assertEquals("topic2", topic);
  }

  @Test
  public void testSortedRetryQueueFunction() throws Exception {

    // added in descending order for testing
    retryConfig =
        RetryConfig.newBuilder()
            .addRetryQueues(queue3)
            .addRetryQueues(queue2)
            .addRetryQueues(queue1)
            .build();

    // Case1: normal just use sorting
    Assert.assertEquals(
        "topic1", RetryUtils.getSortedRetryQueues(retryConfig).get(0).getRetryQueueTopic());
  }

  @Test
  public void testIsRetryConfigAvailable() {

    // added in descending order for testing
    retryConfig =
        RetryConfig.newBuilder()
            .addRetryQueues(queue3)
            .addRetryQueues(queue2)
            .addRetryQueues(queue1)
            .setRetryEnabled(true)
            .build();

    Job job = Job.newBuilder().setRetryConfig(retryConfig).build();

    Assert.assertTrue(RetryUtils.isTieredRetryConfigAvailable(job));
    // Case2: when retryConfig is empty
    retryConfig = RetryConfig.newBuilder().build();
    job = Job.newBuilder().setRetryConfig(retryConfig).build();

    RetryConfig retryConfig12 = RetryConfig.newBuilder().build();
    Assert.assertFalse(RetryUtils.isTieredRetryConfigAvailable(job));
  }

  @Test
  public void testFindRetryQueueWithTopicName() throws Exception {
    retryConfig =
        RetryConfig.newBuilder()
            .addRetryQueues(queue3)
            .addRetryQueues(queue2)
            .addRetryQueues(queue1)
            .setRetryEnabled(true)
            .build();
    Job job = Job.newBuilder().setRetryConfig(retryConfig).build();

    // Case1: Not null
    Assert.assertEquals(queue1, RetryUtils.findRetryQueueWithTopicName(job, "topic1").get());

    // Case2: null due to non-exist retry queue
    Assert.assertFalse(RetryUtils.findRetryQueueWithTopicName(job, "topic4").isPresent());

    // Case3: null due to no configured retry queue
    Assert.assertFalse(
        RetryUtils.findRetryQueueWithTopicName(Job.newBuilder().build(), "topic1").isPresent());
  }

  @Test
  public void testIsRetryTopic() {
    Job jobWithRetryQueue =
        Job.newBuilder()
            .setRetryConfig(
                RetryConfig.newBuilder()
                    .addRetryQueues(
                        RetryQueue.newBuilder().setRetryQueueTopic("foo__bar__retry").build())
                    .setRetryEnabled(true)
                    .build())
            .build();

    Assert.assertTrue(RetryUtils.isRetryTopic("foo__bar__retry", jobWithRetryQueue));
  }

  @Test
  public void testIsDLQTopic() {
    Job jobWithDLQ =
        Job.newBuilder()
            .setRpcDispatcherTask(
                RpcDispatcherTask.newBuilder().setDlqTopic("foo__bar__dlq").build())
            .build();
    Assert.assertTrue(RetryUtils.isDLQTopic("foo__bar__dlq", jobWithDLQ));
  }
}
