package com.uber.data.kafka.consumerproxy.common;

import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class StructuredTagsTest extends FievelTestBase {
  @Test
  public void test() {
    Map<String, String> tagMap =
        StructuredTags.builder()
            .setKafkaCluster("cluster")
            .setKafkaGroup("group")
            .setKafkaTopic("topic")
            .setKafkaPartition(1)
            .setJobType("test1")
            .setMode("test2")
            .setDestination("test3")
            .build();
    Assert.assertEquals(7, tagMap.size());
    Assert.assertEquals("cluster", tagMap.get(StructuredFields.KAFKA_CLUSTER));
    Assert.assertEquals("group", tagMap.get(StructuredFields.KAFKA_GROUP));
    Assert.assertEquals("topic", tagMap.get(StructuredFields.KAFKA_TOPIC));
    Assert.assertEquals("1", tagMap.get(StructuredFields.KAFKA_PARTITION));
    Assert.assertEquals("test1", tagMap.get("job_type"));
    Assert.assertEquals("test2", tagMap.get("mode"));
    Assert.assertEquals("unknown", tagMap.get("destination"));

    tagMap = StructuredTags.builder().setDestination("grpc://test4").build();
    Assert.assertEquals(1, tagMap.size());
    Assert.assertEquals("test4", tagMap.get("destination"));
  }
}
