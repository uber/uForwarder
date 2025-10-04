package com.uber.data.kafka.consumerproxy.client.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * ConsumerMetadata contains accessor methods for retrieving Kafka metadata from gRPC metadata.
 *
 * <p>Kafka Metadata is stored as the following gRPC headers:
 *
 * <ul>
 *   <li>{@code kafka-topic} contains topic name
 *   <li>{@code kafka-consumergroup} contains consumer group name
 *   <li>{@code kafka-partition} contains partition id
 *   <li>{@code kafka-offset} contains offset value
 *   <li>{@code kafka-retrycount} contains retry count
 *   <li>{@code kafka-tracing-info} contains retry/dlq topic name,partition id,offset value. Purely
 *       for tracing purpose.
 * </ul>
 *
 * <br>
 * Note: we retrieve Grpc Metadata from Grpc context It only work after Grpc servers is initialized
 * with serverInterceptor(). </br>
 */
public final class ConsumerMetadata {
  private static final Metadata EMPTY_METADATA = new Metadata();
  private static final Context.Key<Metadata> GRPC_METADATA_KEY =
      Context.keyWithDefault("grpc-metadata-key", EMPTY_METADATA);
  private static final ServerInterceptor SERVER_INTERCEPTOR =
      new ServerInterceptor() {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
          Context ctx = Context.current();
          ctx = ctx.withValue(GRPC_METADATA_KEY, headers);
          return Contexts.interceptCall(ctx, call, headers, next);
        }
      };
  private static MetadataAdapter METADATA_MANAGER =
      new MetadataAdapter(() -> GRPC_METADATA_KEY.get());

  /**
   * Gets a server interceptor that propagates {@link Metadata} through {@link Context} to message
   * handler Usage:
   *
   * <p>Server grpcServer = NettyServerBuilder.forPort(8080) .addService(
   * ServerInterceptors.intercept( serverServiceDefinitionBuilder.build(),
   * ConsumerMetadata.serverInterceptor())) .build();
   *
   * @return a GRPC server interceptor
   */
  public static ServerInterceptor serverInterceptor() {
    return SERVER_INTERCEPTOR;
  }

  /**
   * @return the Kafka topic name associated with this request or empty string if one was not found.
   */
  public static String getTopic() {
    return METADATA_MANAGER.getTopic();
  }

  /**
   * @return the Kafka consumer group associated with this request or empty string if one was not
   *     found.
   */
  public static String getConsumerGroup() {
    return METADATA_MANAGER.getConsumerGroup();
  }

  /**
   * @return the Kafka partition associated with this request or -1 if one was not found.
   */
  public static int getPartition() {
    return METADATA_MANAGER.getPartition();
  }

  /**
   * @return the Kafka offset associated with this request or -1 if one was not found.
   */
  public static long getOffset() {
    return METADATA_MANAGER.getOffset();
  }

  /**
   * @return the Kafka retry count associated with this request or -1 if one was not found.
   */
  public static long getRetryCount() {
    return METADATA_MANAGER.getRetryCount();
  }

  /**
   * Gets the header value with the supplied header key(case-irrelevant).
   *
   * @param headerKey the key for identifying the header
   * @return the string representing the header value
   */
  public static String getHeader(String headerKey) {
    return METADATA_MANAGER.getHeader(headerKey);
  }

  /**
   * This field should be used only for tracing information and no business logic should be
   * implemented based on this.
   *
   * @return the Retry/DLQ Kafka topic name associated with this request or empty string if the
   *     message is not consumed from retry/dlq queue.
   */
  public static String getTracingInfo() {
    return METADATA_MANAGER.getTracingInfo();
  }

  @VisibleForTesting
  protected static void runWithMetadata(Metadata headers, Runnable runnable) {
    Context ctx = Context.current();
    ctx = ctx.withValue(GRPC_METADATA_KEY, headers);
    Context previous = ctx.attach();
    try {
      runnable.run();
    } finally {
      ctx.detach(previous);
    }
  }
}
