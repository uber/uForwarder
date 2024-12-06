package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.CommandType;
import com.uber.fievel.testing.base.FievelTestBase;
import net.logstash.logback.argument.StructuredArgument;
import org.junit.Assert;
import org.junit.Test;

public class StructuredLoggingTest extends FievelTestBase {

  @Test
  public void test() {
    StructuredLogging structuredLogging = new StructuredLogging();
    StructuredArgument keyValue = StructuredLogging.kafkaTopic("topic");
    Assert.assertEquals("kafka_topic=topic", keyValue.toString());
    keyValue = StructuredLogging.kafkaCluster("cluster");
    Assert.assertEquals("kafka_cluster=cluster", keyValue.toString());
    keyValue = StructuredLogging.kafkaGroup("group");
    Assert.assertEquals("kafka_group=group", keyValue.toString());
    keyValue = StructuredLogging.kafkaOffset(1000);
    Assert.assertEquals("kafka_offset=1000", keyValue.toString());
    keyValue = StructuredLogging.commandType("RUN");
    Assert.assertEquals("command_type=RUN", keyValue.toString());
    keyValue = StructuredLogging.commandType(CommandType.COMMAND_TYPE_RUN_JOB);
    Assert.assertEquals("command_type=COMMAND_TYPE_RUN_JOB", keyValue.toString());
    keyValue = StructuredLogging.workerId(6);
    Assert.assertEquals("worker_id=6", keyValue.toString());
    keyValue = StructuredLogging.jobId(8);
    Assert.assertEquals("job_id=8", keyValue.toString());
    keyValue = StructuredLogging.pod("pod");
    Assert.assertEquals("pod=pod", keyValue.toString());
  }
}
