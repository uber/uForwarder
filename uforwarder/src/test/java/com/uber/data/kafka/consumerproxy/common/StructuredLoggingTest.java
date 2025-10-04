package com.uber.data.kafka.consumerproxy.common;

import net.logstash.logback.argument.StructuredArgument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StructuredLoggingTest {

  @Test
  public void test() {
    new StructuredLogging();
    StructuredArgument keyValue = StructuredLogging.rpcRoutingKey("value1");
    Assertions.assertEquals("rpc_routing_key=value1", keyValue.toString());
    keyValue = StructuredLogging.destination("value2");
    Assertions.assertEquals("destination=value2", keyValue.toString());
    keyValue = StructuredLogging.dispatcher("value3");
    Assertions.assertEquals("dispatcher=value3", keyValue.toString());
    keyValue = StructuredLogging.jobType("value4");
    Assertions.assertEquals("job_type=value4", keyValue.toString());
    keyValue = StructuredLogging.offsetGap(5L);
    Assertions.assertEquals("offset_gap=5", keyValue.toString());
    keyValue = StructuredLogging.spiffeId("value6");
    Assertions.assertEquals("spiffe_id=value6", keyValue.toString());
    keyValue = StructuredLogging.virtualPartition(6);
    Assertions.assertEquals("virtual_partition=6", keyValue.toString());
    keyValue = StructuredLogging.workloadBasedWorkerCount(8);
    Assertions.assertEquals("worker_count=8", keyValue.toString());
    keyValue = StructuredLogging.workerId(1000L);
    Assertions.assertEquals("worker_id=1000", keyValue.toString());
    keyValue = StructuredLogging.jobPod("pod");
    Assertions.assertEquals("job_pod=pod", keyValue.toString());
  }
}
