package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import static com.uber.data.kafka.datatransfer.common.MetadataUtils.metadataInterceptor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.uber.data.kafka.datatransfer.HeartbeatRequest;
import com.uber.data.kafka.datatransfer.HeartbeatResponse;
import com.uber.data.kafka.datatransfer.MasterWorkerServiceGrpc;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class DedupHeaderInterceptorTest extends FievelTestBase {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private GrpcRequest request;
  private Scope scope;

  @Before
  public void setup() {
    request = Mockito.mock(GrpcRequest.class);
    scope = new NoopScope();
    Mockito.when(request.getConsumergroup()).thenReturn("group");
    Mockito.when(request.getTopic()).thenReturn("topic");
  }

  private final ServerInterceptor mockServerInterceptor =
      mock(
          ServerInterceptor.class,
          delegatesTo(
              new ServerInterceptor() {
                @Override
                public <ReqT, RespT> Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                  return next.startCall(call, headers);
                }
              }));

  @Test
  public void clientHeaderDeliveredToServer() throws IOException {
    // Generate a unique in-process server name.
    String serverName = InProcessServerBuilder.generateName();
    // Create a client channel and register for automatic graceful shutdown.
    ManagedChannel channel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());

    // Create a server, add service, start, and register for automatic graceful shutdown.
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(
                ServerInterceptors.intercept(
                    new MasterWorkerServiceGrpc.MasterWorkerServiceImplBase() {},
                    mockServerInterceptor))
            .build()
            .start());
    Channel c = ClientInterceptors.intercept(channel, new DedupHeaderInterceptor(scope, request));
    // this is expected to be deleted
    c = ClientInterceptors.intercept(c, metadataInterceptor("k1", "v1"));
    c = ClientInterceptors.intercept(c, metadataInterceptor("k1", "v2"));
    c = ClientInterceptors.intercept(c, metadataInterceptor("k2", "v3"));
    MasterWorkerServiceGrpc.MasterWorkerServiceBlockingStub blockingStub =
        MasterWorkerServiceGrpc.newBlockingStub(c);
    ArgumentCaptor<Metadata> metadataCaptor = ArgumentCaptor.forClass(Metadata.class);

    try {
      blockingStub.heartbeat(HeartbeatRequest.getDefaultInstance());
      fail();
    } catch (StatusRuntimeException expected) {
      // expected because method is not implemented
    }

    verify(mockServerInterceptor)
        .interceptCall(
            ArgumentMatchers.<ServerCall<HeartbeatRequest, HeartbeatResponse>>any(),
            metadataCaptor.capture(),
            ArgumentMatchers.any());
    assertEquals(
        "v2",
        metadataCaptor.getValue().get(Metadata.Key.of("k1", Metadata.ASCII_STRING_MARSHALLER)));
    int counter = 0;
    for (String value :
        metadataCaptor.getValue().getAll(Metadata.Key.of("k1", Metadata.ASCII_STRING_MARSHALLER))) {
      counter++;
    }
    assertEquals(1, counter);
    assertEquals(
        "v3",
        metadataCaptor.getValue().get(Metadata.Key.of("k2", Metadata.ASCII_STRING_MARSHALLER)));
  }
}
