package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumer.DLQMetadata;
import com.uber.data.kafka.datatransfer.FlowControl;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.KafkaConsumerTask;
import com.uber.data.kafka.datatransfer.ResqConfig;
import com.uber.data.kafka.datatransfer.RetryConfig;
import com.uber.data.kafka.datatransfer.RetryQueue;
import com.uber.data.kafka.datatransfer.RpcDispatcherTask;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.fievel.testing.base.FievelTestBase;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Before;

public abstract class ProcessorTestBase extends FievelTestBase {
  protected static String KEY = "key";
  protected static String VALUE = "value";
  protected static String CLUSTER = "cluster";
  protected static String GROUP = "group";
  protected static String TOPIC = "topic";
  protected static int PARTITION = 10;
  protected static long OFFSET = 20;
  protected static String RETRY_TOPIC = "topic__retry";
  protected static String DLQ_TOPIC = "topic__dlq";
  protected static String RESQ_TOPIC = "topic_resq";
  protected static String MUTTLEY_ROUTING_KEY = "muttley://routing-key";
  protected static long TIMESTAMP = 0L;
  protected static TimestampType TIMESTAMP_TYPE = TimestampType.CREATE_TIME;
  protected static byte[] TRACE_ID = "0".getBytes(StandardCharsets.UTF_8);
  protected static byte[] SPAN_ID = "0".getBytes(StandardCharsets.UTF_8);
  protected static int RETRY_PARTITION = 7;

  protected ConsumerRecord<byte[], byte[]> consumerRecord;
  protected ConsumerRecord<byte[], byte[]> emptyConsumerRecord;
  protected RecordHeaders headers;
  protected Job job;
  protected Job retryJob;
  protected ProcessorMessage processorMessage;
  protected CoreInfra infra;

  @Before
  public void setup() throws Exception {
    infra = CoreInfra.NOOP;
    job = newJob(TOPIC, PARTITION);
    retryJob = newJob(RETRY_TOPIC, RETRY_PARTITION);

    headers =
        new RecordHeaders() {
          {
            add("traceid", TRACE_ID);
            add("spanid", SPAN_ID);
          }
        };
    consumerRecord =
        new ConsumerRecord<>(
            TOPIC,
            PARTITION,
            OFFSET,
            TIMESTAMP,
            TIMESTAMP_TYPE,
            null,
            KEY.length(),
            VALUE.length(),
            KEY.getBytes(),
            VALUE.getBytes(),
            headers);
    emptyConsumerRecord =
        new ConsumerRecord<>(
            TOPIC,
            PARTITION,
            OFFSET,
            TIMESTAMP,
            TIMESTAMP_TYPE,
            null,
            KEY.length(),
            "".length(),
            KEY.getBytes(),
            "".getBytes(),
            headers);
    processorMessage = ProcessorMessage.of(consumerRecord, job, infra, new MessageStub());
  }

  Job newJob(String topic, int partition) {
    return Job.newBuilder()
        .setJobId(100)
        .setFlowControl(
            FlowControl.newBuilder()
                .setBytesPerSec(10)
                .setBytesPerSec(10000)
                .setMaxInflightMessages(1)
                .build())
        .setKafkaConsumerTask(
            KafkaConsumerTask.newBuilder()
                .setCluster(CLUSTER)
                .setConsumerGroup(GROUP)
                .setTopic(topic)
                .setPartition(partition)
                .build())
        .setRpcDispatcherTask(
            RpcDispatcherTask.newBuilder()
                .setUri(MUTTLEY_ROUTING_KEY)
                .setDlqTopic(DLQ_TOPIC)
                .setRpcTimeoutMs(1000)
                .setMaxRpcTimeouts(1)
                .build())
        .setRetryConfig(
            RetryConfig.newBuilder()
                .setRetryEnabled(true)
                .addRetryQueues(
                    RetryQueue.newBuilder()
                        .setMaxRetryCount(10)
                        .setRetryQueueTopic(RETRY_TOPIC)
                        .build())
                .build())
        .setResqConfig(
            ResqConfig.newBuilder()
                .setResqEnabled(true)
                .setResqTopic(RESQ_TOPIC)
                .setResqCluster(CLUSTER)
                .build())
        .build();
  }

  ProcessorMessage newProcessMessage(TopicPartitionOffset topicPartitionOffset) throws Exception {
    Job job = newJob(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    consumerRecord =
        new ConsumerRecord<>(
            topicPartitionOffset.getTopic(),
            topicPartitionOffset.getPartition(),
            topicPartitionOffset.getOffset(),
            TIMESTAMP,
            TIMESTAMP_TYPE,
            null,
            KEY.length(),
            VALUE.length(),
            KEY.getBytes(),
            VALUE.getBytes(),
            headers);
    return ProcessorMessage.of(consumerRecord, job, infra, new MessageStub());
  }

  ProcessorMessage newEmptyProcessMessage(TopicPartitionOffset topicPartitionOffset)
      throws Exception {
    Job job = newJob(topicPartitionOffset.getTopic(), topicPartitionOffset.getPartition());
    emptyConsumerRecord =
        new ConsumerRecord<>(
            topicPartitionOffset.getTopic(),
            topicPartitionOffset.getPartition(),
            topicPartitionOffset.getOffset(),
            TIMESTAMP,
            TIMESTAMP_TYPE,
            null,
            KEY.length(),
            "".length(),
            KEY.getBytes(),
            "".getBytes(),
            headers);
    return ProcessorMessage.of(emptyConsumerRecord, job, infra, new MessageStub());
  }

  ProcessorMessage newRetryProcessMessage(Job job, long offset, DLQMetadata dlqMetadata)
      throws Exception {
    return ProcessorMessage.of(
        new ConsumerRecord<>(
            job.getKafkaConsumerTask().getTopic(),
            job.getKafkaConsumerTask().getPartition(),
            offset,
            TIMESTAMP,
            TIMESTAMP_TYPE,
            null,
            dlqMetadata.toByteArray().length,
            VALUE.length(),
            dlqMetadata.toByteArray(),
            VALUE.getBytes(),
            headers),
        job,
        infra,
        new MessageStub());
  }
}
