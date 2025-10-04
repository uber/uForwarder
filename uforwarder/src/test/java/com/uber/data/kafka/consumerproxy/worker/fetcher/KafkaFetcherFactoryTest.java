package com.uber.data.kafka.consumerproxy.worker.fetcher;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.ThreadRegister;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcher;
import com.uber.data.kafka.datatransfer.worker.fetchers.kafka.KafkaFetcherConfiguration;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Stopwatch;
import com.uber.m3.tally.Timer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class KafkaFetcherFactoryTest {
  private static final String ORIGINAL_TOPIC = "topic";
  private static final String RETRY_TOPIC_1 = "topic__group__1__retry";
  private static final String RETRY_TOPIC_2 = "topic__group__2__retry";
  private static final String RETRY_CLUSTER = "kloak-dca1-dlq";
  private static final String CONSUMER_GROUP = "group";
  private static final String THREAD_ID = "thread-id";

  private CoreInfra infra;
  private ThreadRegister threadRegister;

  @BeforeEach
  public void setup() {
    Scope scope = Mockito.mock(Scope.class);
    infra = CoreInfra.builder().withScope(scope).build();
    Counter counter = Mockito.mock(Counter.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Timer timer = Mockito.mock(Timer.class);
    Stopwatch stopwatch = Mockito.mock(Stopwatch.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(timer.start()).thenReturn(stopwatch);

    threadRegister = Mockito.mock(ThreadRegister.class);
    Mockito.when(threadRegister.register(Mockito.any())).then(AdditionalAnswers.returnsFirstArg());
  }

  @Test
  public void test() throws Exception {
    KafkaFetcherFactory kafkaFetcherFactory =
        new KafkaFetcherFactory(new KafkaFetcherConfiguration());
    // create a OriginalTopicKafkaFetcher
    Job job1 =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setConsumerGroup(CONSUMER_GROUP)
                    .setTopic(ORIGINAL_TOPIC)
                    .build())
            .build();
    Assertions.assertTrue(
        kafkaFetcherFactory.create(job1, THREAD_ID, threadRegister, infra) instanceof KafkaFetcher);

    // create a DlqTopicKafkaFetcher
    Job job2 =
        Job.newBuilder(job1)
            .setRpcDispatcherTask(
                RpcDispatcherTask.newBuilder().setDlqTopic(ORIGINAL_TOPIC).build())
            .build();
    Assertions.assertTrue(
        kafkaFetcherFactory.create(job2, THREAD_ID, threadRegister, infra) instanceof KafkaFetcher);

    // create a RetryTopicKafkaFetcher
    Job job3 =
        Job.newBuilder(job1)
            .setRpcDispatcherTask(
                RpcDispatcherTask.newBuilder().setRetryQueueTopic(RETRY_TOPIC_1).build())
            .build();
    Assertions.assertTrue(
        kafkaFetcherFactory.create(job3, THREAD_ID, threadRegister, infra) instanceof KafkaFetcher);

    Mockito.verify(threadRegister, Mockito.times(3)).register(Mockito.any());
  }

  @Test
  public void testCreateRetryQueueKafkaFetcher() throws Exception {
    Job job =
        Job.newBuilder()
            .setRpcDispatcherTask(
                RpcDispatcherTask.newBuilder()
                    .setRetryQueueTopic(RETRY_TOPIC_1)
                    .setRetryCluster(RETRY_CLUSTER)
                    .build())
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster(RETRY_CLUSTER)
                    .setConsumerGroup(CONSUMER_GROUP)
                    .setTopic(RETRY_TOPIC_1)
                    .setPartition(1)
                    .build())
            .build();
    KafkaFetcherFactory retryKafkaFetcherFactory =
        new KafkaFetcherFactory(new KafkaFetcherConfiguration());
    KafkaFetcher<byte[], byte[]> retryFetcherInstance =
        retryKafkaFetcherFactory.create(job, THREAD_ID, threadRegister, infra);

    Assertions.assertTrue(retryFetcherInstance instanceof KafkaFetcher);

    Mockito.verify(threadRegister, Mockito.times(1)).register(Mockito.any());
  }

  @Test
  public void testCreateRetryQueueKafkaFetcherWithTRQConfig() throws Exception {
    Job job =
        Job.newBuilder()
            .setKafkaConsumerTask(
                KafkaConsumerTask.newBuilder()
                    .setCluster(RETRY_CLUSTER)
                    .setConsumerGroup(CONSUMER_GROUP)
                    .setTopic(RETRY_TOPIC_2)
                    .setPartition(1)
                    .build())
            .setRetryConfig(
                RetryConfig.newBuilder()
                    .addRetryQueues(
                        RetryQueue.newBuilder()
                            .setRetryQueueTopic(RETRY_TOPIC_1)
                            .setRetryCluster(RETRY_CLUSTER)
                            .build())
                    .addRetryQueues(
                        RetryQueue.newBuilder()
                            .setRetryQueueTopic(RETRY_TOPIC_2)
                            .setRetryCluster(RETRY_CLUSTER)
                            .build())
                    .setRetryEnabled(true)
                    .build())
            .build();
    KafkaFetcherFactory retryKafkaFetcherFactory =
        new KafkaFetcherFactory(new KafkaFetcherConfiguration());
    KafkaFetcher<byte[], byte[]> retryFetcherInstance =
        retryKafkaFetcherFactory.create(job, THREAD_ID, threadRegister, infra);

    Assertions.assertTrue(retryFetcherInstance instanceof KafkaFetcher);

    Mockito.verify(threadRegister, Mockito.times(1)).register(Mockito.any());
  }
}
