package com.uber.data.kafka.consumerproxy.common;

import com.uber.fievel.testing.base.FievelTestBase;
import net.logstash.logback.argument.StructuredArgument;
import org.junit.Assert;
import org.junit.Test;

public class StructuredLoggingTest extends FievelTestBase {

  @Test
  public void test() {
    new StructuredLogging();
    StructuredArgument keyValue = StructuredLogging.rpcRoutingKey("value1");
    Assert.assertEquals("rpc_routing_key=value1", keyValue.toString());
    keyValue = StructuredLogging.destination("value2");
    Assert.assertEquals("destination=value2", keyValue.toString());
    keyValue = StructuredLogging.dispatcher("value3");
    Assert.assertEquals("dispatcher=value3", keyValue.toString());
    keyValue = StructuredLogging.jobType("value4");
    Assert.assertEquals("job_type=value4", keyValue.toString());
    keyValue = StructuredLogging.offsetGap(5L);
    Assert.assertEquals("offset_gap=5", keyValue.toString());
    keyValue = StructuredLogging.spiffeId("value6");
    Assert.assertEquals("spiffe_id=value6", keyValue.toString());
    keyValue = StructuredLogging.virtualPartition(6);
    Assert.assertEquals("virtual_partition=6", keyValue.toString());
    keyValue = StructuredLogging.workloadBasedWorkerCount(8);
    Assert.assertEquals("worker_count=8", keyValue.toString());
    keyValue = StructuredLogging.workerId(1000L);
    Assert.assertEquals("worker_id=1000", keyValue.toString());
  }
}
