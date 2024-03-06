package com.uber.data.kafka.consumerproxy.common;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.RoutingUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.m3.tally.Scope;

/** Utility class for metrics */
public class MetricsUtils {
  private MetricsUtils() {}

  /**
   * Returns a scope tagged with job metadata
   *
   * @param scope the scope
   * @param job the job
   * @return the scope tagged with job metadata
   */
  public static Scope jobScope(Scope scope, Job job) {
    return jobScope(scope, job, RoutingUtils.extractAddress(job.getRpcDispatcherTask().getUri()));
  }

  /**
   * Returns a scope tagged with job metadata
   *
   * @param scope the scope
   * @param job the job
   * @param rpcAddress the rpcAddress of RpcDispatcherTask of the job
   * @return the scope tagged with job metadata
   */
  public static Scope jobScope(Scope scope, Job job, String rpcAddress) {
    final String group = job.getKafkaConsumerTask().getConsumerGroup();
    final String cluster = job.getKafkaConsumerTask().getCluster();
    final String topic = job.getKafkaConsumerTask().getTopic();
    final String partition = Integer.toString(job.getKafkaConsumerTask().getPartition());
    final String routingKey = rpcAddress;
    return scope.tagged(
        ImmutableMap.of(
            StructuredFields.KAFKA_GROUP,
            group,
            StructuredFields.KAFKA_CLUSTER,
            cluster,
            StructuredFields.KAFKA_TOPIC,
            topic,
            StructuredFields.KAFKA_PARTITION,
            partition,
            StructuredFields.URI,
            routingKey));
  }
}
