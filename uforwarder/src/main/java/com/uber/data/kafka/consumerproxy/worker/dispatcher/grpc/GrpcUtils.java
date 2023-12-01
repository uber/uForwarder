package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.config.GrpcDispatcherConfiguration;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.RoutingUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.m3.tally.Scope;
import net.logstash.logback.argument.StructuredArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcUtils.class);

  private GrpcUtils() {}

  /**
   * Gets the timeout given job and configuration details, together with timeout count
   *
   * @param job the job config
   * @param configuration the dispatcher config
   * @param timeoutCount the message timeout count
   * @param scope metrics scope
   * @return timeout in ms
   */
  public static long getTimeout(
      Job job, GrpcDispatcherConfiguration configuration, long timeoutCount, Scope scope) {
    long rpcTimeoutMs = job.getRpcDispatcherTask().getRpcTimeoutMs();
    if (rpcTimeoutMs < configuration.getMinRpcTimeoutMs()) {
      rpcTimeoutMs = configuration.getMinRpcTimeoutMs();
    } else if (rpcTimeoutMs > configuration.getMaxRpcTimeoutMs()) {
      rpcTimeoutMs = configuration.getMaxRpcTimeoutMs();
    }
    // adjust the rpc timeout value based on dispatchAttempt;
    long copyOfTimeoutCount = timeoutCount;
    while (rpcTimeoutMs < configuration.getMaxRpcTimeoutMs() && copyOfTimeoutCount > 0) {
      rpcTimeoutMs = rpcTimeoutMs << 1;
      if (rpcTimeoutMs >= configuration.getMaxRpcTimeoutMs()) {
        rpcTimeoutMs = configuration.getMaxRpcTimeoutMs();
        break;
      }
      copyOfTimeoutCount--;
    }
    if (timeoutCount > 0) {
      String cluster = job.getKafkaConsumerTask().getCluster();
      String group = job.getKafkaConsumerTask().getConsumerGroup();
      String topic = job.getKafkaConsumerTask().getTopic();
      int partition = job.getKafkaConsumerTask().getPartition();
      String routingKey = RoutingUtils.extractAddress(job.getRpcDispatcherTask().getUri());
      LOGGER.info(
          "using adjusted rpc timeout",
          StructuredLogging.rpcRoutingKey(routingKey),
          StructuredLogging.kafkaCluster(cluster),
          StructuredLogging.kafkaGroup(group),
          StructuredLogging.kafkaTopic(topic),
          StructuredLogging.kafkaPartition(partition),
          StructuredArguments.keyValue("adjusted_rpc_timeout", rpcTimeoutMs),
          StructuredArguments.keyValue("timeout_count", timeoutCount));
      Scope taggedScope =
          scope.tagged(
              ImmutableMap.of(
                  StructuredFields.URI,
                  routingKey,
                  StructuredFields.KAFKA_GROUP,
                  group,
                  StructuredFields.KAFKA_CLUSTER,
                  cluster,
                  StructuredFields.KAFKA_TOPIC,
                  topic,
                  StructuredFields.KAFKA_PARTITION,
                  Integer.toString(partition)));
      taggedScope.gauge(MetricNames.TIMEOUT_COUNT).update(timeoutCount);
      taggedScope.gauge(MetricNames.ADJUSTED_RPC_TIMEOUT).update(rpcTimeoutMs);
    }
    return rpcTimeoutMs;
  }

  private static class MetricNames {
    static final String TIMEOUT_COUNT = "dispatcher.grpc.timeout-count";
    static final String ADJUSTED_RPC_TIMEOUT = "dispatcher.grpc.adjusted-rpc-timeout";
  }
}
