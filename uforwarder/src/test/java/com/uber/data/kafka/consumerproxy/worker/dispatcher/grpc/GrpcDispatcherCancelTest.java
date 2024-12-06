package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.uber.data.kafka.consumerproxy.config.GrpcDispatcherConfiguration;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.fievel.testing.base.FievelTestBase;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcDispatcherCancelTest extends FievelTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcDispatcherCancelTest.class);
  private GrpcDispatcher dispatcher;
  private CoreInfra infra;
  private GrpcDispatcherConfiguration config;
  private Server server;
  private MethodDescriptor<ByteString, Empty> methodDescriptor;
  private GrpcManagedChannelPool channel;
  private String callee;
  private MessageStub stub;
  private int port;
  private CountDownLatch latch;
  private volatile int finishCount;

  @Before
  public void setUp() throws IOException {
    this.config = new GrpcDispatcherConfiguration();
    this.infra = CoreInfra.NOOP;
    this.callee = "callee";
    this.methodDescriptor = methodDescriptor("test/testAPI", new BytesMarshaller());
    this.latch = new CountDownLatch(1);
    final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    server =
        ServerBuilder.forPort(0)
            .addService(
                ServerServiceDefinition.builder("test")
                    .addMethod(
                        ServerMethodDefinition.create(
                            methodDescriptor,
                            ServerCalls.asyncUnaryCall(
                                (request, responseObserver) -> {
                                  latch.countDown();
                                  executorService.schedule(
                                      () -> {
                                        finishCount++;
                                        responseObserver.onNext(Empty.getDefaultInstance());
                                        responseObserver.onCompleted();
                                      },
                                      2,
                                      TimeUnit.SECONDS);
                                })))
                    .build())
            .build();
    server.start();
    port = server.getPort();
    this.channel =
        new GrpcManagedChannelPool(
            () -> ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build(),
            1,
            10);
    this.stub = new MessageStub();
    this.dispatcher =
        new GrpcDispatcher(
            infra, config, channel, methodDescriptor, callee, GrpcFilter.NOOP, "caller");
    dispatcher.start();
  }

  @After
  public void close() {
    dispatcher.stop();
    server.shutdown();
  }

  @Test
  public void testComplete() throws ExecutionException, InterruptedException {
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(1000000);
    Job job = jobBuilder.build();
    GrpcRequest grpcRequest = newGrpcRequest();
    GrpcResponse response =
        dispatcher.submit(ItemAndJob.of(grpcRequest, job)).toCompletableFuture().get();
    Assert.assertEquals(0, latch.getCount());
    Assert.assertEquals(1, finishCount);
    Assert.assertEquals(Status.Code.OK, response.status().getCode());
  }

  @Test
  public void testCancelled() throws Throwable {
    // cancel before rpc reach to server
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(1000000);
    Job job = jobBuilder.build();
    GrpcRequest grpcRequest = newGrpcRequest();
    stub.cancel(DispatcherResponse.Code.RETRY);
    GrpcResponse response =
        dispatcher.submit(ItemAndJob.of(grpcRequest, job)).toCompletableFuture().get();
    Assert.assertEquals(Status.Code.CANCELLED, response.status().getCode());
    Assert.assertEquals(1, latch.getCount());
    Assert.assertEquals(0, finishCount);
  }

  @Test
  public void testCallThenCancel() throws Throwable {
    // cancel after rpc reach to server
    Job.Builder jobBuilder = Job.newBuilder();
    jobBuilder.getRpcDispatcherTaskBuilder().setRpcTimeoutMs(1000000);
    Job job = jobBuilder.build();
    GrpcRequest grpcRequest = newGrpcRequest();
    CompletableFuture<GrpcResponse> future =
        dispatcher.submit(ItemAndJob.of(grpcRequest, job)).toCompletableFuture();
    latch.await();
    stub.cancel(DispatcherResponse.Code.RETRY);
    GrpcResponse response = future.get();
    Assert.assertEquals(Status.Code.CANCELLED, response.status().getCode());
    Assert.assertEquals(0, latch.getCount());
    Assert.assertEquals(0, finishCount);
  }

  private GrpcRequest newGrpcRequest() {
    return new GrpcRequest(
        "group",
        "topic",
        0,
        0,
        stub,
        0,
        0,
        0,
        "physicaltopic",
        "physicalCluster",
        0,
        0,
        new RecordHeaders(),
        "value".getBytes(),
        "key".getBytes());
  }

  private static <Req> MethodDescriptor<Req, Empty> methodDescriptor(
      String methodName, MethodDescriptor.Marshaller<Req> reqMarshaller) {
    return MethodDescriptor.<Req, Empty>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName(methodName)
        .setSampledToLocalTracing(true) // copy defaults from grpc proto generated method descriptor
        .setRequestMarshaller(reqMarshaller)
        .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
        .build();
  }

  private static final class BytesMarshaller implements MethodDescriptor.Marshaller<ByteString> {

    @Override
    public InputStream stream(ByteString value) {
      return value.newInput();
    }

    @Override
    public ByteString parse(InputStream stream) {
      try {
        return ByteString.readFrom(stream);
      } catch (IOException e) {
        LOGGER.error("failed to parse InputStream into ByteString", e);
        throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withCause(e));
      }
    }
  }
}
