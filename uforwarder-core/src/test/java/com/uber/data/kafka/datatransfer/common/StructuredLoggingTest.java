package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.CommandType;
import net.logstash.logback.argument.StructuredArgument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StructuredLoggingTest {

  @Test
  public void test() {
    StructuredLogging structuredLogging = new StructuredLogging();
    StructuredArgument keyValue = StructuredLogging.kafkaTopic("topic");
    Assertions.assertEquals("kafka_topic=topic", keyValue.toString());
    keyValue = StructuredLogging.kafkaCluster("cluster");
    Assertions.assertEquals("kafka_cluster=cluster", keyValue.toString());
    keyValue = StructuredLogging.kafkaGroup("group");
    Assertions.assertEquals("kafka_group=group", keyValue.toString());
    keyValue = StructuredLogging.kafkaOffset(1000);
    Assertions.assertEquals("kafka_offset=1000", keyValue.toString());
    keyValue = StructuredLogging.commandType("RUN");
    Assertions.assertEquals("command_type=RUN", keyValue.toString());
    keyValue = StructuredLogging.commandType(CommandType.COMMAND_TYPE_RUN_JOB);
    Assertions.assertEquals("command_type=COMMAND_TYPE_RUN_JOB", keyValue.toString());
    keyValue = StructuredLogging.workerId(6);
    Assertions.assertEquals("worker_id=6", keyValue.toString());
    keyValue = StructuredLogging.jobId(8);
    Assertions.assertEquals("job_id=8", keyValue.toString());
    keyValue = StructuredLogging.pod("pod");
    Assertions.assertEquals("pod=pod", keyValue.toString());
  }
}
