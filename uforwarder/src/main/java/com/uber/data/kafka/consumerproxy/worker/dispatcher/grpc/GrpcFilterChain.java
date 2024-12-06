package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.datatransfer.Job;
import io.grpc.Channel;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Chained Grpc filter Wrapped a list of {@link GrpcFilter}
 *
 * <p>while handle request, internal {@link GrpcFilter} are invoked in ascending order
 *
 * <p>while handle response, internal {@link GrpcFilter} are invoked in descending order
 */
public class GrpcFilterChain implements GrpcFilter {
  private final ImmutableList<GrpcFilter> filterList;

  private GrpcFilterChain(List<GrpcFilter> grpcFilters) {
    filterList = ImmutableList.copyOf(grpcFilters);
  }

  @Override
  public Channel interceptChannel(Channel channel, GrpcRequest grpcRequest, String... tags) {
    for (GrpcFilter grpcFilter : filterList) {
      channel = grpcFilter.interceptChannel(channel, grpcRequest, tags);
    }
    return channel;
  }

  @Override
  public Optional<Status> tryHandleRequest(GrpcRequest grpcRequest, Job job) {
    for (GrpcFilter grpcFilter : filterList) {
      Optional<Status> status = grpcFilter.tryHandleRequest(grpcRequest, job);
      if (status.isPresent()) {
        return status;
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<DispatcherResponse.Code> tryHandleError(
      Throwable t, GrpcRequest request, Map<String, String> tags) {
    for (GrpcFilter grpcFilter : Lists.reverse(filterList)) {
      Optional<DispatcherResponse.Code> code = grpcFilter.tryHandleError(t, request, tags);
      if (code.isPresent()) {
        return code;
      }
    }
    return Optional.empty();
  }

  /**
   * New builder of {@link GrpcFilter}.
   *
   * @return the builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** Builder builds a {@link GrpcFilter} or NOOP when there is no nested filter */
  public static class Builder {
    private List<GrpcFilter> filterList = new ArrayList<>();

    private Builder() {}

    /**
     * adds a nested filter
     *
     * @param grpcFilter the grpc filter
     * @return the builder
     */
    Builder add(GrpcFilter grpcFilter) {
      filterList.add(grpcFilter);
      return this;
    }

    /**
     * builds filter
     *
     * @return the grpc filter
     */
    public GrpcFilter build() {
      if (filterList.isEmpty()) {
        return GrpcFilter.NOOP;
      }

      return new GrpcFilterChain(filterList);
    }
  }
}
