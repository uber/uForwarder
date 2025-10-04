package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import static org.mockito.Mockito.mockConstruction;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.uber.data.kafka.consumerproxy.config.GrpcDispatcherConfiguration;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.DynamicConfiguration;
import com.uber.data.kafka.datatransfer.common.context.ContextManager;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcDispatcherTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcDispatcherTest.class);
  private static Headers HEADERS = new RecordHeaders();
  private CoreInfra infra;
  private GrpcDispatcherConfiguration config;
  private String callee;
  private GrpcManagedChannelPool channel;
  private MethodDescriptor<ByteString, Empty> methodDescriptor;
  private long timeoutMs;
  private GrpcDispatcher dispatcher;
  private GrpcFilter grpcFilter;
  private GrpcRequest grpcRequest;
  private DynamicConfiguration dynamicConfiguration;
  private MessageStub messageStub;
  private MessageStub mockStub;
  private GrpcManagedChannelPool.Metrics channelPoolMetrics;
  private ExecutorService executorService;

  @BeforeEach
  public void setup() {
    ContextManager contextManager = Mockito.mock(ContextManager.class);
    messageStub = new MessageStub();
    this.config = new GrpcDispatcherConfiguration();
    this.callee = "muttley://kafka-consumer-proxy";
    this.channel = Mockito.mock(GrpcManagedChannelPool.class);
    this.channelPoolMetrics = Mockito.mock(GrpcManagedChannelPool.Metrics.class);
    this.methodDescriptor = GrpcDispatcher.buildMethodDescriptor("group/topic");
    this.grpcFilter = Mockito.mock(GrpcFilter.class);
    this.grpcRequest = Mockito.mock(GrpcRequest.class);
    this.dynamicConfiguration = Mockito.mock(DynamicConfiguration.class);
    this.infra = CoreInfra.builder().withContextManager(contextManager).build();
    this.executorService = Executors.newSingleThreadExecutor();
    Mockito.when(channel.getMetrics()).thenReturn(channelPoolMetrics);
    Mockito.when(contextManager.wrap(Mockito.any(CompletableFuture.class)))
        .thenAnswer(
            (Answer<CompletableFuture>)
                invocation -> invocation.getArgument(0, CompletableFuture.class));
    Mockito.when(
            grpcFilter.interceptChannel(
                Mockito.any(Channel.class),
                Mockito.any(GrpcRequest.class),
                ArgumentMatchers.<String>any()))
        .thenReturn(channel);
    Mockito.when(
            grpcFilter.tryHandleError(
                Mockito.any(Throwable.class), Mockito.any(GrpcRequest.class), Mockito.any()))
        .thenAnswer(
            (Answer<?>)
                invocation -> {
                  Throwable t = invocation.getArgument(0, Throwable.class);
                  Status status = Status.fromThrowable(t);
                  if (status == Status.UNKNOWN) {
                    return Optional.of(DispatcherResponse.Code.DLQ);
                  }
                  return Optional.empty();
                });
    Mockito.when(
            grpcFilter.tryHandleRequest(Mockito.any(GrpcRequest.class), Mockito.any(Job.class)))
        .thenAnswer(
            (Answer<?>)
                invocation -> {
                  GrpcRequest request = invocation.getArgument(0, GrpcRequest.class);
                  if (request.getConsumergroup().equals("invalidGroup")) {
                    return Optional.of(Status.UNAUTHENTICATED);
                  } else {
                    return Optional.empty();
                  }
                });
    Mockito.when(dynamicConfiguration.isHeaderAllowed(Mockito.anyMap())).thenReturn(true);
    this.dispatcher =
        new GrpcDispatcher(
            infra,
            Optional.of(executorService),
            config,
            channel,
            methodDescriptor,
            callee,
            grpcFilter,
            "caller");
    this.timeoutMs = 10000;
  }

  @Test
  public void testLifecycle() {
    Assertions.assertFalse(dispatcher.isRunning());
    dispatcher.start();
    Assertions.assertTrue(dispatcher.isRunning());
    dispatcher.stop();
    Assertions.assertFalse(dispatcher.isRunning());
  }

  @Test
  public void testSubmitNullMessage() {
    Assertions.assertTrue(dispatcher.submit(null).toCompletableFuture().isCompletedExceptionally());
  }

  @Test
  public void testSubmit() throws ExecutionException, InterruptedException {
    final Map<GrpcDispatcher.ResponseStreamObserver, List<Object>> constructorArgs =
        new HashMap<>();
    try (MockedConstruction<GrpcDispatcher.ResponseStreamObserver> mock =
        mockConstruction(
            GrpcDispatcher.ResponseStreamObserver.class,
            (out, context) -> constructorArgs.put(out, new ArrayList<>(context.arguments())))) {
      ClientCall<ByteString, Empty> clientCall = Mockito.mock(ClientCall.class);
      final String serviceIdentity = "spiffe://kafka-consumer/test/proxy";
      Mockito.when(channel.newCall(Mockito.any(), Mockito.any()))
          .thenReturn((ClientCall) clientCall);
      Job.Builder jobBuilder = Job.newBuilder();
      jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(1000000);
      jobBuilder.getSecurityConfigBuilder().addServiceIdentities(serviceIdentity);
      jobBuilder.getSecurityConfigBuilder().setIsSecure(true);
      Job job = jobBuilder.build();
      GrpcRequest grpcRequest =
          new GrpcRequest(
              "group",
              "topic",
              0,
              0,
              messageStub,
              0,
              0,
              0,
              "physicaltopic",
              "physicalCluster",
              0,
              0,
              HEADERS,
              "value".getBytes(),
              "key".getBytes());
      Assertions.assertFalse(
          dispatcher.submit(ItemAndJob.of(grpcRequest, job)).toCompletableFuture().isDone());
      Mockito.verify(infra.contextManager(), Mockito.times(1))
          .wrap(Mockito.any(CompletableFuture.class));
      Mockito.verify(grpcFilter, Mockito.times(1))
          .interceptChannel(
              Mockito.any(Channel.class),
              Mockito.any(GrpcRequest.class),
              ArgumentMatchers.<String>any());
      GrpcDispatcher.ResponseStreamObserver mockObserver = mock.constructed().get(0);
      CompletableFuture future = (CompletableFuture) constructorArgs.get(mockObserver).get(1);
      future.complete(GrpcResponse.of());
      Assertions.assertTrue(
          grpcRequest
              .getFuture()
              .thenApply(response -> response.status() == Status.OK ? true : false)
              .toCompletableFuture()
              .get());
    }
  }

  @SuppressWarnings({"CheckReturnValue"})
  @Test
  public void testCancelDispatch() {
    try (MockedStatic<Context> staticContext = Mockito.mockStatic(Context.class)) {
      Context.CancellableContext mockContext = Mockito.mock(Context.CancellableContext.class);
      staticContext.when(() -> Context.current()).thenReturn(mockContext);
      Mockito.when(mockContext.withCancellation()).thenReturn(mockContext);
      ClientCall<ByteString, Empty> clientCall = Mockito.mock(ClientCall.class);
      MessageStub messageStub = new MessageStub();
      final String serviceIdentity = "spiffe://kafka-consumer/test/proxy";
      Mockito.when(channel.newCall(Mockito.any(), Mockito.any()))
          .thenReturn((ClientCall) clientCall);
      Job.Builder jobBuilder = Job.newBuilder();
      jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(1000000);
      jobBuilder.getSecurityConfigBuilder().addServiceIdentities(serviceIdentity);
      jobBuilder.getSecurityConfigBuilder().setIsSecure(true);
      Job job = jobBuilder.build();
      GrpcRequest grpcRequest =
          new GrpcRequest(
              "group",
              "topic",
              0,
              0,
              messageStub,
              0,
              0,
              0,
              "physicaltopic",
              "physicalCluster",
              0,
              0,
              HEADERS,
              "value".getBytes(),
              "key".getBytes());
      Assertions.assertFalse(
          dispatcher.submit(ItemAndJob.of(grpcRequest, job)).toCompletableFuture().isDone());
      messageStub.cancel(DispatcherResponse.Code.RETRY);
      Mockito.verify(mockContext, Mockito.times(1)).cancel(null);
    }
  }

  @Test
  public void testFilterHandleRequest() throws ExecutionException, InterruptedException {
    ClientCall<ByteString, Empty> clientCall = Mockito.mock(ClientCall.class);
    final String serviceIdentity = "spiffe://kafka-consumer/test/proxy";
    Mockito.when(channel.newCall(Mockito.any(), Mockito.any())).thenReturn((ClientCall) clientCall);
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(1000000);
    jobBuilder.getSecurityConfigBuilder().addServiceIdentities(serviceIdentity);
    jobBuilder.getSecurityConfigBuilder().setIsSecure(true);
    Job job = jobBuilder.build();
    GrpcRequest grpcRequest =
        new GrpcRequest(
            "invalidGroup",
            "topic",
            0,
            0,
            messageStub,
            0,
            0,
            0,
            "physicaltopic",
            "physicalCluster",
            0,
            0,
            HEADERS,
            "value".getBytes(),
            "key".getBytes());
    CompletableFuture<GrpcResponse> future =
        dispatcher.submit(ItemAndJob.of(grpcRequest, job)).toCompletableFuture();
    Assertions.assertTrue(future.isDone());
    Mockito.verify(infra.contextManager(), Mockito.times(1))
        .wrap(Mockito.any(CompletableFuture.class));
    Mockito.verify(grpcFilter, Mockito.times(1))
        .tryHandleRequest(Mockito.eq(grpcRequest), Mockito.eq(job));
    Mockito.verify(grpcFilter, Mockito.never())
        .interceptChannel(
            Mockito.any(Channel.class),
            Mockito.any(GrpcRequest.class),
            ArgumentMatchers.<String>any());
    Assertions.assertEquals(Status.UNAUTHENTICATED, future.get().status());
  }

  @Test
  public void testSubmitNullKey() {
    ClientCall<ByteString, Empty> clientCall = Mockito.mock(ClientCall.class);
    Mockito.when(channel.newCall(Mockito.any(), Mockito.any())).thenReturn((ClientCall) clientCall);
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(1000000);
    Job job = jobBuilder.build();
    GrpcRequest grpcRequest =
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            messageStub,
            0,
            0,
            0,
            "value".getBytes(),
            "physicaltopic",
            "physicalCluster",
            0,
            0,
            HEADERS);
    Assertions.assertFalse(
        dispatcher.submit(ItemAndJob.of(grpcRequest, job)).toCompletableFuture().isDone());
    Mockito.verify(infra.contextManager(), Mockito.times(1))
        .wrap(Mockito.any(CompletableFuture.class));
    Mockito.verify(grpcFilter, Mockito.times(1))
        .interceptChannel(
            Mockito.any(Channel.class),
            Mockito.any(GrpcRequest.class),
            ArgumentMatchers.<String>any());
  }

  @Test
  public void testMethodDescriptor() {
    MethodDescriptor.Marshaller<ByteString> marshaller = methodDescriptor.getRequestMarshaller();
    String string = "string";
    Assertions.assertEquals(
        string,
        marshaller
            .parse(marshaller.stream(ByteString.copyFrom(string, Charsets.UTF_8)))
            .toStringUtf8());
  }

  @Test
  public void testResponseStreamObserverOk() throws Exception {
    GrpcRequest grpcRequest =
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            messageStub,
            0,
            0,
            0,
            "physicaltopic",
            "physicalCluster",
            0,
            0,
            HEADERS,
            "value".getBytes(),
            "key".getBytes());
    CompletableFuture<GrpcResponse> future = grpcRequest.getFuture();
    GrpcDispatcher.ResponseStreamObserver responseStreamObserver =
        dispatcher.new ResponseStreamObserver(future, timeoutMs, grpcRequest);
    responseStreamObserver.onNext(Empty.getDefaultInstance());
    Assertions.assertFalse(future.isDone());
    responseStreamObserver.onCompleted();
    Assertions.assertEquals(Status.Code.OK, future.get().status().getCode());
    Assertions.assertTrue(
        future
            .thenApply(response -> response.status() == Status.OK ? true : false)
            .toCompletableFuture()
            .get());
  }

  @Test
  public void testResponseStreamObserverOnError() throws Exception {
    CompletableFuture<GrpcResponse> future = new CompletableFuture<>();
    GrpcDispatcher.ResponseStreamObserver responseStreamObserver =
        dispatcher.new ResponseStreamObserver(future, timeoutMs, grpcRequest);
    responseStreamObserver.onError(Status.FAILED_PRECONDITION.asRuntimeException());
    GrpcResponse response = future.get();
    Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, response.status().getCode());
    Assertions.assertFalse(response.isOverDue());
  }

  @Test
  public void testResponseStreamObserverOverDue() throws Exception {
    CompletableFuture<GrpcResponse> future = new CompletableFuture<>();
    GrpcDispatcher.ResponseStreamObserver responseStreamObserver =
        dispatcher.new ResponseStreamObserver(future, -1, grpcRequest);
    responseStreamObserver.onError(Status.UNAVAILABLE.asRuntimeException());
    GrpcResponse response = future.get();
    Assertions.assertEquals(Status.Code.UNAVAILABLE, response.status().getCode());
    Assertions.assertTrue(response.isOverDue());
  }

  @Test
  public void testResponseStreamObserverWithFilter()
      throws ExecutionException, InterruptedException {
    CompletableFuture<GrpcResponse> future = new CompletableFuture<>();
    GrpcDispatcher.ResponseStreamObserver responseStreamObserver =
        dispatcher.new ResponseStreamObserver(future, 100000, grpcRequest);
    responseStreamObserver.onError(Status.UNKNOWN.asRuntimeException());
    GrpcResponse response = future.get();
    Assertions.assertEquals(Status.Code.UNKNOWN, response.status().getCode());
    Assertions.assertFalse(response.isOverDue());
    Assertions.assertEquals(DispatcherResponse.Code.DLQ, response.code().get());
  }

  @Test
  public void testResponseStreamObserverOnErrorWithConsumerResponse() throws Exception {
    Map<String, DispatcherResponse.Code> cases =
        ImmutableMap.of(
            "Retry",
            DispatcherResponse.Code.RETRY,
            "Stash",
            DispatcherResponse.Code.DLQ,
            "Skip",
            DispatcherResponse.Code.SKIP,
            "xxyy",
            DispatcherResponse.Code.INVALID);
    for (Map.Entry<String, DispatcherResponse.Code> entry : cases.entrySet()) {
      CompletableFuture<GrpcResponse> future = new CompletableFuture<>();
      GrpcDispatcher.ResponseStreamObserver responseStreamObserver =
          dispatcher.new ResponseStreamObserver(future, timeoutMs, grpcRequest);
      Metadata metadata = new Metadata();
      metadata.put(
          Metadata.Key.of("kafka-action", Metadata.ASCII_STRING_MARSHALLER), entry.getKey());
      responseStreamObserver.onError(
          new StatusRuntimeException(Status.FAILED_PRECONDITION, metadata));
      Assertions.assertEquals(Status.Code.FAILED_PRECONDITION, future.get().status().getCode());
      Assertions.assertEquals(entry.getValue(), future.get().code().get());
    }
  }

  @Test
  public void testRuntimeExceptionWhenStartClientCall()
      throws ExecutionException, InterruptedException {
    ClientCall<ByteString, Empty> clientCall = Mockito.mock(ClientCall.class);
    MessageStub messageStub = new MessageStub();
    Mockito.doThrow(new RuntimeException("NPE"))
        .when(clientCall)
        .start(Mockito.any(), Mockito.any());
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(1000000);
    Job job = jobBuilder.build();
    GrpcRequest grpcRequest =
        new GrpcRequest(
            "group",
            "topic",
            0,
            0,
            messageStub,
            0,
            0,
            0,
            "physicaltopic",
            "physicalCluster",
            0,
            0,
            HEADERS,
            "value".getBytes(),
            "key".getBytes());
    CompletableFuture<GrpcResponse> future =
        dispatcher.submit(ItemAndJob.of(grpcRequest, job)).toCompletableFuture();
    Assertions.assertTrue(future.isDone());
    Assertions.assertEquals(Status.UNKNOWN.getCode(), future.get().status().getCode());
    // Make sure current attempt has been reset
    Assertions.assertTrue(messageStub.newAttempt() != null);
  }

  @Test
  public void testConstructor() {
    Assertions.assertNotNull(
        new GrpcDispatcher(
            infra,
            Optional.of(executorService),
            config,
            "caller",
            "dns:///127.0.0.1",
            "procedure",
            grpcFilter));
  }
}
