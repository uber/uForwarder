package com.uber.data.kafka.datatransfer.common;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.CommandType;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.WorkerState;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import net.logstash.logback.argument.StructuredArgument;
import net.logstash.logback.argument.StructuredArguments;
import org.apache.kafka.common.TopicPartition;

/**
 * StructuredLogging utility for generating logback StructuredArgument pairs for structured logging.
 *
 * <p>We use static methods so that we can use the Java type system to ensure that the key-value
 * pairs have consistent type and therefore will not collide during ELK ingestion.
 *
 * <p>https://docs.google.com/document/d/1kOn2iYvwv_dZjADQCtXVSQR4LkXNt5GcawR2jqMU8Qw/edit#heading=h.nojf84n695pb
 */
public class StructuredLogging extends StructuredFields {

  public static final String WORKER_ID = "worker_id";
  public static final String JOB_STATE = "job_state";
  public static final String COMMAND_TYPE = "command_type";
  public static final String REASON = "reason";
  public static final String ACTION = "action";
  public static final String FROM_MASTER_HOST = "from_master_host";
  public static final String TO_MASTER_HOST = "to_master_host";
  public static final String PIPELINE_ID = "pipeline_id";
  public static final String PIPELINE_JOB = "pipeline_job";
  public static final String CONTROLLABLE_COMMAND = "controllable_command";

  private static final String WORKER_HOST = "worker_host";
  private static final String MASTER_HOST_PORT = "master_host_port";
  private static final String WORKER_STAGE = "worker_stage";
  private static final String ZK_PATH = "zk_path";
  private static final String COUNT = "count";
  private static final String METHOD = "method";
  private static final String KAFKA_CLUSTER = "kafka_cluster";
  private static final String METRIC_NAME = "metric_name";
  private static final String INTERVAL_MS = "interval_ms";
  private static final String ATTEMPT = "attempt";
  private static final String FROM_MESSAGES_PER_SEC = "from_messages_per_sec";
  private static final String TO_MESSAGES_PER_SEC = "to_messages_per_sec";
  private static final String WORKER_ID_STR = "worker_id_str";
  private static final String RESOURCE = "resource";
  private static final String ID = "id";
  private static final String TO_STATE = "to_state";
  private static final String JOB_ID = "job_id";
  private static final String FROM_ID = "from_id";
  private static final String TO_ID = "to_id";
  private static final String FROM_STATE = "from_state";
  private static final String JOB_ID_STR = "job_id_str";
  private static final String JOB_GROUP_ID = "job_group_id";
  private static final String JOB_TYPE = "job_type";
  private static final String WORKER_STATE = "worker_state";
  private static final String IDEAL_STATE = "ideal_state";
  private static final String CURRENT_STATE = "current_state";
  private static final String KAFKA_TOPIC_PARTITION_LIST = "kafka_topic_partition_list";
  private static final String ZONE_INTENTION = "zone_intention";

  private static final String WORKLOAD_SCALE = "workload_scale";

  public static StructuredArgument kafkaTopic(String topic) {
    return StructuredArguments.keyValue(KAFKA_TOPIC, topic);
  }

  public static StructuredArgument kafkaTopics(List<String> topicList) {
    return StructuredArguments.keyValue(KAFKA_TOPIC_LIST, topicList.toString());
  }

  public static StructuredArgument kafkaGroup(String group) {
    return StructuredArguments.keyValue(KAFKA_GROUP, group);
  }

  public static StructuredArgument kafkaCluster(String cluster) {
    return StructuredArguments.keyValue(KAFKA_CLUSTER, cluster);
  }

  public static StructuredArgument metricName(String metricName) {
    return StructuredArguments.keyValue(METRIC_NAME, metricName);
  }

  public static StructuredArgument zkPath(String path) {
    return StructuredArguments.keyValue(ZK_PATH, path);
  }

  public static StructuredArgument count(int count) {
    return StructuredArguments.keyValue(COUNT, count);
  }

  public static StructuredArgument count(long count) {
    return StructuredArguments.keyValue(COUNT, count);
  }

  public static StructuredArgument method(String method) {
    return StructuredArguments.keyValue(METHOD, method);
  }

  public static StructuredArgument intervalMs(long ms) {
    return StructuredArguments.keyValue(INTERVAL_MS, ms);
  }

  public static StructuredArgument attempt(long attempt) {
    return StructuredArguments.keyValue(ATTEMPT, attempt);
  }

  public static StructuredArgument fromMessagesPerSec(double rate) {
    return StructuredArguments.keyValue(FROM_MESSAGES_PER_SEC, rate);
  }

  public static StructuredArgument toMessagesPerSec(double rate) {
    return StructuredArguments.keyValue(TO_MESSAGES_PER_SEC, rate);
  }

  public static StructuredArgument workerId(long id) {
    return StructuredArguments.keyValue(WORKER_ID, id);
  }

  public static StructuredArgument workerId(String id) {
    return StructuredArguments.keyValue(WORKER_ID_STR, id);
  }

  public static StructuredArgument resource(String resource) {
    return StructuredArguments.keyValue(RESOURCE, resource);
  }

  public static StructuredArgument id(String id) {
    return StructuredArguments.keyValue(ID, id);
  }

  public static StructuredArgument jobId(long id) {
    return StructuredArguments.keyValue(JOB_ID, id);
  }

  public static StructuredArgument fromId(long id) {
    return StructuredArguments.keyValue(FROM_ID, id);
  }

  public static StructuredArgument toId(long id) {
    return StructuredArguments.keyValue(TO_ID, id);
  }

  public static StructuredArgument fromState(String state) {
    return StructuredArguments.keyValue(FROM_STATE, state);
  }

  public static StructuredArgument toState(String toState) {
    return StructuredArguments.keyValue(TO_STATE, toState);
  }

  public static StructuredArgument uri(String uri) {
    return StructuredArguments.keyValue(URI, uri);
  }

  public static StructuredArgument jobId(String id) {
    return StructuredArguments.keyValue(JOB_ID_STR, id);
  }

  public static StructuredArgument jobGroupId(String id) {
    return StructuredArguments.keyValue(JOB_GROUP_ID, id);
  }

  public static StructuredArgument jobState(JobState state) {
    return StructuredArguments.keyValue(JOB_STATE, state.toString());
  }

  public static StructuredArgument workerState(WorkerState state) {
    return StructuredArguments.keyValue(WORKER_STATE, state.toString());
  }

  public static StructuredArgument idealState(JobState state) {
    return StructuredArguments.keyValue(IDEAL_STATE, state.toString());
  }

  public static StructuredArgument currentState(JobState state) {
    return StructuredArguments.keyValue(CURRENT_STATE, state.toString());
  }

  public static StructuredArgument commandType(CommandType cmdType) {
    return StructuredArguments.keyValue(COMMAND_TYPE, cmdType.toString());
  }

  public static StructuredArgument commandType(String cmdType) {
    return StructuredArguments.keyValue(COMMAND_TYPE, cmdType);
  }

  public static StructuredArgument reason(String reason) {
    return StructuredArguments.keyValue(REASON, reason);
  }

  public static StructuredArgument kafkaPartition(int partition) {
    return StructuredArguments.keyValue(KAFKA_PARTITION, partition);
  }

  public static StructuredArgument kafkaOffset(long offset) {
    return StructuredArguments.keyValue(KAFKA_OFFSET, offset);
  }

  public static StructuredArgument kafkaTopicPartitions(
      Collection<TopicPartition> topicPartitionList) {
    String[] topicPartitionArray =
        topicPartitionList.stream().map(tp -> tp.toString()).toArray(String[]::new);
    return StructuredArguments.keyValue(KAFKA_TOPIC_PARTITION_LIST, topicPartitionArray);
  }

  public static StructuredArgument masterHostPort(String masterHostPort) {
    return StructuredArguments.keyValue(MASTER_HOST_PORT, masterHostPort);
  }

  public static StructuredArgument workerHost(String workerHost) {
    return StructuredArguments.keyValue(WORKER_HOST, workerHost);
  }

  public static StructuredArgument fromMasterHost(String fromMasterHost) {
    return StructuredArguments.keyValue(FROM_MASTER_HOST, fromMasterHost);
  }

  public static StructuredArgument toMasterHost(String toMasterHost) {
    return StructuredArguments.keyValue(TO_MASTER_HOST, toMasterHost);
  }

  public static StructuredArgument pipelineId(String pipelineId) {
    return StructuredArguments.keyValue(PIPELINE_ID, pipelineId);
  }

  public static StructuredArgument pipelineJob(Job job) {
    String jobStr;
    try {
      jobStr = JsonFormat.printer().print(job);
    } catch (InvalidProtocolBufferException e) {
      jobStr = "invalidProto";
    }
    return StructuredArguments.keyValue(PIPELINE_JOB, jobStr);
  }

  public static StructuredArgument controllableCommand(String command) {
    return StructuredArguments.keyValue(CONTROLLABLE_COMMAND, command);
  }

  public static StructuredArgument workerStage(String stage) {
    return StructuredArguments.keyValue(WORKER_STAGE, stage);
  }

  public static StructuredArgument action(String action) {
    return StructuredArguments.keyValue(ACTION, action);
  }

  public static StructuredArgument jobType(String jobType) {
    return StructuredArguments.keyValue(JOB_TYPE, jobType);
  }

  public static StructuredArgument topicPartitionOffsets(
      Map<TopicPartition, Long> topicPartitionOffsetMap) {
    return StructuredArguments.keyValue(
        "topicPartitionOffset",
        topicPartitionOffsetMap
            .entrySet()
            .stream()
            .map(e -> String.format("%s:%d", e.getKey().toString(), e.getValue()))
            .toArray(String[]::new));
  }

  public static StructuredArgument zoneIntention(String zoneIntention) {
    return StructuredArguments.keyValue(ZONE_INTENTION, zoneIntention);
  }

  public static StructuredArgument workloadScale(double scale) {
    return StructuredArguments.keyValue(WORKLOAD_SCALE, scale);
  }

  public static StructuredArgument headerKey(String headerKey) {
    return StructuredArguments.keyValue(HEADER_KEY, headerKey);
  }

  public static StructuredArgument headerValue(byte[] headerValue) {
    return StructuredArguments.keyValue(HEADER_VALUE, new String(headerValue));
  }
}
