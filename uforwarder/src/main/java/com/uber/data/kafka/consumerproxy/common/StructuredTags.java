package com.uber.data.kafka.consumerproxy.common;

import com.uber.data.kafka.datatransfer.common.RoutingUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.m3.util.ImmutableMap;
import java.util.Map;

public class StructuredTags extends StructuredFields {
  private static final String JOB_TYPE = "job_type";
  private static final String DESTINATION = "destination";
  private static final String MODE = "mode";

  private final ImmutableMap.Builder<String, String> mapBuilder;

  StructuredTags() {
    this.mapBuilder = new ImmutableMap.Builder<>();
  }

  public static StructuredTags builder() {
    return new StructuredTags();
  }

  public StructuredTags setDestination(String destination) {
    mapBuilder.put(DESTINATION, RoutingUtils.extractAddress(destination));
    return this;
  }

  public StructuredTags setMode(String type) {
    mapBuilder.put(MODE, type);
    return this;
  }

  public StructuredTags setKafkaGroup(String kafkaGroup) {
    mapBuilder.put(KAFKA_GROUP, kafkaGroup);
    return this;
  }

  public StructuredTags setKafkaCluster(String kafkaCluster) {
    mapBuilder.put(KAFKA_CLUSTER, kafkaCluster);
    return this;
  }

  public StructuredTags setKafkaTopic(String kafkaTopic) {
    mapBuilder.put(KAFKA_TOPIC, kafkaTopic);
    return this;
  }

  public StructuredTags setKafkaPartition(int kafkaPartition) {
    mapBuilder.put(KAFKA_PARTITION, Integer.toString(kafkaPartition));
    return this;
  }

  public StructuredTags setJobType(String jobType) {
    mapBuilder.put(JOB_TYPE, jobType);
    return this;
  }

  public Map<String, String> build() {
    return this.mapBuilder.build();
  }
}
