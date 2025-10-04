package com.uber.data.kafka.consumerproxy.common;

import com.uber.data.kafka.datatransfer.common.StructuredFields;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StructuredTagsTest {
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
    Assertions.assertEquals(7, tagMap.size());
    Assertions.assertEquals("cluster", tagMap.get(StructuredFields.KAFKA_CLUSTER));
    Assertions.assertEquals("group", tagMap.get(StructuredFields.KAFKA_GROUP));
    Assertions.assertEquals("topic", tagMap.get(StructuredFields.KAFKA_TOPIC));
    Assertions.assertEquals("1", tagMap.get(StructuredFields.KAFKA_PARTITION));
    Assertions.assertEquals("test1", tagMap.get("job_type"));
    Assertions.assertEquals("test2", tagMap.get("mode"));
    Assertions.assertEquals("unknown", tagMap.get("destination"));

    tagMap = StructuredTags.builder().setDestination("grpc://test4").build();
    Assertions.assertEquals(1, tagMap.size());
    Assertions.assertEquals("test4", tagMap.get("destination"));
  }
}
