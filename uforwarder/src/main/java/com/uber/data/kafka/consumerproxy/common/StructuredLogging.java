package com.uber.data.kafka.consumerproxy.common;

import net.logstash.logback.argument.StructuredArgument;
import net.logstash.logback.argument.StructuredArguments;

/**
 * StructuredLogging utility for generating logback KeyValue pairs for structured logging.
 *
 * <p>We use static methods so that we can use the Java type system to ensure that the key-value
 * pairs have consistent type and therefore will not collide during ELK ingestion.
 *
 * <p>https://docs.google.com/document/d/1kOn2iYvwv_dZjADQCtXVSQR4LkXNt5GcawR2jqMU8Qw/edit#heading=h.nojf84n695pb
 *
 * <p>We extend StructureLogging from datatransfer for consistency of logging key-value pairs.
 */
public class StructuredLogging extends com.uber.data.kafka.datatransfer.common.StructuredLogging {

  public static final String DISPATCHER = "dispatcher";
  public static final String DESTINATION = "destination";

  private static final String RPC_ROUTING_KEY = "rpc_routing_key";
  private static final String SPIFFE_ID = "spiffe_id";
  private static final String OFFSET_GAP = "offset_gap";
  private static final String JOB_TYPE = "job_type";

  private static final String VIRTUAL_PARTITION = "virtual_partition";

  private static final String WORKLOAD_BASED_WORKER_COUNT = "worker_count";

  private static final String WORKER_ID = "worker_id";

  private static final String SKIPPED_VIRTUAL_PARTITION = "skipped_virtual_partition";

  public static StructuredArgument rpcRoutingKey(String rpcRoutingKey) {
    return StructuredArguments.keyValue(RPC_ROUTING_KEY, rpcRoutingKey);
  }

  public static StructuredArgument spiffeId(String spiffeId) {
    return StructuredArguments.keyValue(SPIFFE_ID, spiffeId);
  }

  public static StructuredArgument destination(String destination) {
    return StructuredArguments.keyValue(DESTINATION, destination);
  }

  public static StructuredArgument dispatcher(String dispatcher) {
    return StructuredArguments.keyValue(DISPATCHER, dispatcher);
  }

  public static StructuredArgument offsetGap(long offsetGap) {
    return StructuredArguments.keyValue(OFFSET_GAP, offsetGap);
  }

  public static StructuredArgument jobType(String jobType) {
    return StructuredArguments.keyValue(JOB_TYPE, jobType);
  }

  public static StructuredArgument virtualPartition(long partitionIdx) {
    return StructuredArguments.keyValue(VIRTUAL_PARTITION, partitionIdx);
  }

  public static StructuredArgument skippedVirtualPartition(long partitionIdx) {
    return StructuredArguments.keyValue(SKIPPED_VIRTUAL_PARTITION, partitionIdx);
  }

  public static StructuredArgument workloadBasedWorkerCount(int workerCount) {
    return StructuredArguments.keyValue(WORKLOAD_BASED_WORKER_COUNT, workerCount);
  }

  public static StructuredArgument workerId(long workerId) {
    return StructuredArguments.keyValue(WORKER_ID, workerId);
  }
}
