package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.datatransfer.Job;
import io.grpc.Channel;
import io.grpc.Status;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class GrpcFilterChainTest {
  private GrpcRequest request = Mockito.mock(GrpcRequest.class);

  @Test
  public void testBuildEmptyFilter() {
    GrpcFilterChain.Builder builder = GrpcFilterChain.newBuilder();
    GrpcFilter grpcFilter = builder.build();
    Assertions.assertEquals(grpcFilter, GrpcFilter.NOOP);
  }

  @Test
  public void testFilterRequests() {
    Channel channel1 = Mockito.mock(Channel.class);
    Channel channel2 = Mockito.mock(Channel.class);
    Channel channel3 = Mockito.mock(Channel.class);
    GrpcRequest request = Mockito.mock(GrpcRequest.class);
    GrpcFilter mockFilter1 = Mockito.mock(GrpcFilter.class);
    GrpcFilter mockFilter2 = Mockito.mock(GrpcFilter.class);
    Mockito.when(mockFilter1.interceptChannel(channel1, request)).thenReturn(channel2);
    Mockito.when(mockFilter2.interceptChannel(channel2, request)).thenReturn(channel3);
    GrpcFilter filter = GrpcFilterChain.newBuilder().add(mockFilter1).add(mockFilter2).build();
    Channel result = filter.interceptChannel(channel1, request);
    Assertions.assertEquals(channel3, result);
  }

  @Test
  public void testFilterErrors() {
    Throwable t = Mockito.mock(Throwable.class);
    GrpcFilter mockFilter1 = Mockito.mock(GrpcFilter.class);
    GrpcFilter mockFilter2 = Mockito.mock(GrpcFilter.class);
    Mockito.when(mockFilter1.tryHandleError(t, request, Collections.EMPTY_MAP))
        .thenReturn(Optional.of(DispatcherResponse.Code.DLQ));
    Mockito.when(mockFilter2.tryHandleError(t, request, Collections.EMPTY_MAP))
        .thenReturn(Optional.empty());
    GrpcFilter filter = GrpcFilterChain.newBuilder().add(mockFilter1).add(mockFilter2).build();
    Optional<DispatcherResponse.Code> code =
        filter.tryHandleError(t, request, Collections.EMPTY_MAP);
    Mockito.verify(mockFilter2, Mockito.times(1)).tryHandleError(t, request, Collections.EMPTY_MAP);
    Assertions.assertEquals(DispatcherResponse.Code.DLQ, code.get());
  }

  @Test
  public void testHandleRequest() {
    Job job = Job.newBuilder().build();
    GrpcFilter mockFilter1 = Mockito.mock(GrpcFilter.class);
    GrpcFilter mockFilter2 = Mockito.mock(GrpcFilter.class);
    Mockito.when(mockFilter1.tryHandleRequest(request, job)).thenReturn(Optional.empty());
    Mockito.when(mockFilter2.tryHandleRequest(request, job))
        .thenReturn(Optional.of(Status.PERMISSION_DENIED));
    GrpcFilter filter = GrpcFilterChain.newBuilder().add(mockFilter1).add(mockFilter2).build();
    Optional<Status> status1 = filter.tryHandleRequest(request, job);
    Mockito.verify(mockFilter1, Mockito.times(1)).tryHandleRequest(request, job);
    Mockito.verify(mockFilter2, Mockito.times(1)).tryHandleRequest(request, job);
    Assertions.assertEquals(Status.PERMISSION_DENIED, status1.get());

    // both filters now return empty
    Mockito.clearInvocations(mockFilter1, mockFilter2);
    Mockito.when(mockFilter2.tryHandleRequest(request, job)).thenReturn(Optional.empty());
    Optional<Status> status2 = filter.tryHandleRequest(request, job);
    Mockito.verify(mockFilter1, Mockito.times(1)).tryHandleRequest(request, job);
    Mockito.verify(mockFilter2, Mockito.times(1)).tryHandleRequest(request, job);
    Assertions.assertFalse(status2.isPresent());
  }
}
