package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.uber.data.kafka.datatransfer.common.StructuredTags;
import com.uber.m3.tally.Scope;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;

/**
 * A client interceptor that deduplicate headers.
 *
 * <p>Example:
 *
 * <p>c = ClientInterceptors.intercept(channel, new DedupHeaderInterceptor(scope, request));
 *
 * <p>// k1 -> v1 header will be removed
 *
 * <p>c = ClientInterceptors.intercept(c, metadataInterceptor("k1", "v1"));
 *
 * <p>c = ClientInterceptors.intercept(c, metadataInterceptor("k1","v2"));
 */
class DedupHeaderInterceptor implements ClientInterceptor {
  private final Scope scope;
  private final GrpcRequest grpcRequest;

  DedupHeaderInterceptor(Scope scope, GrpcRequest grpcRequest) {
    this.scope = scope;
    this.grpcRequest = grpcRequest;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        for (String key : headers.keys()) {
          Key metadataKey;
          if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
            metadataKey = Key.of(key, Metadata.BINARY_BYTE_MARSHALLER);
          } else {
            metadataKey = Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
          }
          int counter = 0;
          Iterable<Object> values = headers.getAll(metadataKey);
          if (values == null) {
            continue;
          }
          for (Object value : values) {
            if (counter > 0) {
              scope
                  .tagged(
                      StructuredTags.builder()
                          .setKafkaTopic(grpcRequest.getTopic())
                          .setKafkaGroup(grpcRequest.getConsumergroup())
                          .setHeader(key)
                          .build())
                  .counter("dispatcher.duplicate.header.removed")
                  .inc(1);
              headers.remove(metadataKey, value);
            }
            counter++;
          }
        }
        super.start(
            new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                responseListener) {},
            headers);
      }
    };
  }
}
