package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.datatransfer.Job;
import io.grpc.Channel;
import io.grpc.Status;
import java.util.Map;
import java.util.Optional;

/** Extension of {@link GrpcFilter} provides capability to decorate requests and handle responses */
public interface GrpcFilter {
  // no operation GrpcFilter
  GrpcFilter NOOP = new GrpcFilter() {};

  /**
   * Intercept channel before sending request
   *
   * @param channel the channel
   * @param grpcRequest the grpc request
   * @param tags the tags
   * @return the channel
   */
  default Channel interceptChannel(Channel channel, GrpcRequest grpcRequest, String... tags) {
    return channel;
  }

  /**
   * try handle request, in order to handle the request returns grpc status otherwise returns
   * Optional.empty()
   *
   * @param grpcRequest the grpc request
   * @param job the job
   * @return the optional
   */
  default Optional<Status> tryHandleRequest(GrpcRequest grpcRequest, Job job) {
    return Optional.empty();
  }

  /**
   * Try handle error
   *
   * <p>if handles error returns dispatcher code otherwise returns empty result
   *
   * <p>if error is not handled by {@link GrpcFilter}, it has to be handled by {@link
   * GrpcDispatcher}
   *
   * @param t the t
   * @param tags the tags
   * @return the optional DispatcherResponse.Code
   */
  default Optional<DispatcherResponse.Code> tryHandleError(
      Throwable t, GrpcRequest request, Map<String, String> tags) {
    return Optional.empty();
  }
}
