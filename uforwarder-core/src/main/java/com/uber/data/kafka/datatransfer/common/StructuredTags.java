package com.uber.data.kafka.datatransfer.common;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Node;
import java.util.Map;

public final class StructuredTags extends StructuredFields {

  private final ImmutableMap.Builder<String, String> mapBuilder;

  private StructuredTags() {
    mapBuilder = new ImmutableMap.Builder<>();
  }

  public static StructuredTags builder() {
    return new StructuredTags();
  }

  public StructuredTags setKafkaGroup(String consumerGroup) {
    this.mapBuilder.put(KAFKA_GROUP, consumerGroup);
    return this;
  }

  public StructuredTags setFetcherType(String fetcherType) {
    this.mapBuilder.put(FETCHER_TYPE, fetcherType);
    return this;
  }

  public StructuredTags setKafkaCluster(String cluster) {
    this.mapBuilder.put(KAFKA_CLUSTER, cluster);
    return this;
  }

  public StructuredTags setKafkaTopic(String topic) {
    this.mapBuilder.put(KAFKA_TOPIC, topic);
    return this;
  }

  public StructuredTags setKafkaPartition(int partition) {
    this.mapBuilder.put(KAFKA_PARTITION, Integer.toString(partition));
    return this;
  }

  public StructuredTags setURI(String uri) {
    this.mapBuilder.put(URI, uri);
    return this;
  }

  public StructuredTags setJobType(String jobType) {
    mapBuilder.put(JOB_TYPE, jobType);
    return this;
  }

  public StructuredTags setError(Throwable e) {
    mapBuilder.put(ERROR_TYPE, e.getClass().getSimpleName());
    return this;
  }

  public StructuredTags setError(String error) {
    mapBuilder.put(ERROR_TYPE, error);
    return this;
  }

  public StructuredTags setJobGroup(String jobGroup) {
    mapBuilder.put(JOB_GROUP, jobGroup);
    return this;
  }

  public StructuredTags setJobId(long jobId) {
    mapBuilder.put(JOB_ID, Long.toString(jobId));
    return this;
  }

  public StructuredTags setHeader(String header) {
    mapBuilder.put(HEADER_KEY, header);
    return this;
  }

  public StructuredTags setPipeline(String pipeline) {
    mapBuilder.put(PIPELINE, pipeline);
    return this;
  }

  public StructuredTags setNode(Node node) {
    this.mapBuilder.put(HOST, node.getHost());
    this.mapBuilder.put(PORT, Integer.toString(node.getPort()));
    return this;
  }

  public StructuredTags setConsumerService(String consumerService) {
    this.mapBuilder.put(CONSUMER_SERVICE_NAME, consumerService);
    return this;
  }

  public Map<String, String> build() {
    return this.mapBuilder.build();
  }
}
