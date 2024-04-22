package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.uber.data.kafka.consumerproxy.common.MetricsUtils;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.config.GrpcDispatcherConfiguration;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.consumerproxy.worker.processor.MessageStub;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.RoutingUtils;
import com.uber.data.kafka.datatransfer.common.StructuredFields;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.instrumentation.Instrumentation;
import com.uber.data.kafka.instrumentation.Utils;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** GrpcDispatcher is used to dispatch messages to other Uber micro services. */
public class GrpcDispatcher implements Sink<GrpcRequest, GrpcResponse> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcDispatcher.class);
  private static final String ERROR_MESSAGE_CANCEL = "Message dispatch request cancelled by client";
  private static final Metadata.Key<String> KAFKA_ACTION_KEY =
      Metadata.Key.of("kafka-action", Metadata.ASCII_STRING_MARSHALLER);
  private static final String RETRY = "Retry";
  private static final String STASH = "Stash";
  private static final String SKIP = "Skip";
  // Limiting retry count in metrics tags to reduce fan out
  private static final int MAX_RETRY_COUNT_IN_METRICS = 30;
  private final CoreInfra infra;

  private final GrpcDispatcherConfiguration configuration;

  private final String callee;

  private final String calleeAddress;

  private final GrpcManagedChannelPool channel;
  private final MethodDescriptor<ByteString, Empty> methodDescriptor;
  private String caller;
  private final AtomicBoolean running;
  private final GrpcFilter grpcFilter;

  @VisibleForTesting
  GrpcDispatcher(
      CoreInfra infra,
      GrpcDispatcherConfiguration configuration,
      GrpcManagedChannelPool channel,
      MethodDescriptor<ByteString, Empty> methodDescriptor,
      String callee,
      GrpcFilter grpcFilter,
      String caller) {
    this.infra = infra;
    this.callee = callee;
    this.calleeAddress = RoutingUtils.extractAddress(callee);
    this.channel = channel;
    this.methodDescriptor = methodDescriptor;
    this.configuration = configuration;
    this.grpcFilter = grpcFilter;
    this.running = new AtomicBoolean(false);
    this.caller = caller;
  }

  public GrpcDispatcher(
      CoreInfra infra,
      GrpcDispatcherConfiguration configuration,
      String caller,
      String callee,
      String procedure,
      GrpcFilter grpcFilter) {
    this(
        infra,
        configuration,
        new GrpcManagedChannelPool(
            () -> ManagedChannelBuilder.forTarget(callee).usePlaintext().disableRetry().build(),
            configuration.getGrpcChannelPoolSize(),
            configuration.getMaxConcurrentStreams()),
        buildMethodDescriptor(procedure),
        callee,
        grpcFilter,
        caller);
  }

  @Override
  public CompletionStage<GrpcResponse> submit(ItemAndJob<GrpcRequest> item) {
    CompletableFuture<GrpcResponse> completableFuture =
        infra.contextManager().wrap(new CompletableFuture<>());
    if (item == null) {
      completableFuture.completeExceptionally(new NullPointerException());
      return completableFuture;
    }
    final GrpcRequest message = item.getItem();
    MessageStub.Attempt attempt = message.getStub().newAttempt();
    if (attempt.isCanceled()) {
      // already canceled
      completableFuture.complete(GrpcResponse.of(Status.CANCELLED));
      return attempt.complete(completableFuture);
    }

    final Job job = item.getJob();
    MetricsUtils.jobScope(infra.scope(), job, calleeAddress)
        .gauge(MetricNames.CHANNEL_USAGE)
        .update(channel.getMetrics().usage());
    MetricsUtils.jobScope(infra.scope(), job, calleeAddress)
        .gauge(MetricNames.CHANNEL_SIZE)
        .update(channel.getMetrics().size());

    String[] tags = extractDispatchMetricsTags(item);
    Context.CancellableContext context = Context.current().withCancellation();
    attempt.onCancel(() -> context.cancel(null));
    return attempt.complete(
        Instrumentation.instrument
            .withExceptionalCompletion(
                LOGGER,
                infra.scope(),
                wrapInContext(
                    context,
                    () -> {
                      Optional<Status> status = grpcFilter.tryHandleRequest(message, job);
                      if (status.isPresent()) {
                        return CompletableFuture.completedFuture(GrpcResponse.of(status.get()));
                      }

                      long timeoutMs =
                          GrpcUtils.getTimeout(
                              job, configuration, message.getTimeoutCount(), infra.scope());

                      Instrumentation.instrument.withStreamObserver(
                          LOGGER,
                          infra.scope(),
                          ClientCalls::asyncUnaryCall,
                          channelWithMetadata(message)
                              .newCall(methodDescriptor, extractCallOptions(item)),
                          message.payload(),
                          newStreamObserver(completableFuture, timeoutMs, message, tags),
                          MetricNames.CALL,
                          tags);
                      return completableFuture;
                    }),
                MetricNames.DISPATCH,
                tags)
            .whenComplete(
                (r, t) -> {
                  // close context to avoid memory leak
                  context.close();
                })
            .thenApply(
                response -> {
                  message.getFuture().complete(response);
                  return response;
                }));
  }

  @Override
  public void start() {
    running.set(true);
    LOGGER.info("started GrpcDispatcher", StructuredLogging.rpcRoutingKey(callee));
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  @Override
  public void stop() {
    running.set(false);
    channel.shutdown();
    LOGGER.info("closed GrpcDispatcher", StructuredLogging.rpcRoutingKey(callee));
  }

  /**
   * Creates stream observer to handle grpc response or grpc error
   *
   * @param future the future to be completed
   * @param timeoutMs the timeout in milliseconds
   * @param tags the tags to emit metrics
   * @return the stream observer
   */
  public StreamObserver<Empty> newStreamObserver(
      CompletableFuture<GrpcResponse> future, long timeoutMs, GrpcRequest message, String... tags) {
    return new ResponseStreamObserver(future, timeoutMs, message, tags);
  }

  /**
   * Creates a new channel with metadata
   *
   * @param grpcRequest the grpc request
   * @return the channel with metadata
   */
  public Channel channelWithMetadata(GrpcRequest grpcRequest) {
    // Dedup interceptor should be the first as interceptors are intercepted in reverse order.
    Channel ret =
        ClientInterceptors.intercept(
            channel, new DedupHeaderInterceptor(infra.scope(), grpcRequest));
    ret = ClientInterceptors.intercept(ret, grpcRequest.metadataInterceptors());
    return grpcFilter.interceptChannel(ret, grpcRequest);
  }

  @VisibleForTesting
  static MethodDescriptor<ByteString, Empty> buildMethodDescriptor(String procedure) {
    return MethodDescriptor.<ByteString, Empty>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setSampledToLocalTracing(true)
        .setFullMethodName(procedure)
        .setRequestMarshaller(
            new MethodDescriptor.Marshaller<ByteString>() {
              @Override
              public InputStream stream(ByteString value) {
                return value.newInput();
              }

              @Override
              public ByteString parse(InputStream stream) {
                try {
                  return ByteString.readFrom(stream);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            })
        .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
        .build();
  }

  /**
   * Extracts a CallOption with supplied message and job information.
   *
   * @param item the object containing both the message and job details
   * @return extracted gRPC CallOptions
   */
  public CallOptions extractCallOptions(ItemAndJob<GrpcRequest> item) {
    long timeoutMs =
        GrpcUtils.getTimeout(
            item.getJob(), configuration, item.getItem().getTimeoutCount(), infra.scope());
    CallOptions callOptions =
        CallOptions.DEFAULT.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    return callOptions;
  }

  /**
   * Extracts a list of tags to be used in metrics.
   *
   * @param item the object containing both the message and job details
   * @return extracted metrics tags
   */
  public String[] extractDispatchMetricsTags(ItemAndJob<GrpcRequest> item) {
    final Job job = item.getJob();
    final GrpcRequest message = item.getItem();
    final String cluster = job.getKafkaConsumerTask().getCluster();
    final String group = job.getKafkaConsumerTask().getConsumerGroup();
    final String topic = job.getKafkaConsumerTask().getTopic();
    final String partition = Integer.toString(job.getKafkaConsumerTask().getPartition());
    final String routingKey = RoutingUtils.extractAddress(job.getRpcDispatcherTask().getUri());
    final long retryCount = message.getRetryCount();
    List<String> tagsList = new ArrayList<>();
    Collections.addAll(
        tagsList,
        StructuredFields.KAFKA_GROUP,
        group,
        StructuredFields.KAFKA_CLUSTER,
        cluster,
        StructuredFields.KAFKA_TOPIC,
        topic,
        StructuredFields.KAFKA_PARTITION,
        partition,
        StructuredFields.URI,
        routingKey);
    if (retryCount <= MAX_RETRY_COUNT_IN_METRICS) {
      tagsList.add(StructuredFields.RETRY_COUNT);
      tagsList.add(Long.toString(retryCount));
    }
    return tagsList.stream().toArray(String[]::new);
  }

  private static <T> Supplier<T> wrapInContext(Context context, Supplier<T> supplier) {
    return () -> {
      Context previous = context.attach();
      try {
        return supplier.get();
      } finally {
        context.detach(previous);
      }
    };
  }

  @VisibleForTesting
  public class ResponseStreamObserver implements StreamObserver<Empty> {

    private final CompletableFuture<GrpcResponse> future;
    private final Map<String, String> tags;
    private final long timeoutMs;
    private final long startNano;
    private final GrpcRequest message;

    ResponseStreamObserver(
        CompletableFuture<GrpcResponse> future,
        long timeoutMs,
        GrpcRequest request,
        String... tags) {
      this.future = future;
      this.timeoutMs = timeoutMs;
      this.startNano = System.nanoTime();
      this.message = request;
      Map<String, String> map = new HashMap<>();
      Utils.copyTags(map, tags);
      this.tags = ImmutableMap.copyOf(map);
    }

    /** generate metric tags for metrics dispatcher.grpc.action */
    private Map<String, String> actionTags(String action, Map<String, String> tags) {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
      return builder.put("action", action).putAll(tags).build();
    }

    @Override
    public void onNext(Empty value) {}

    @Override
    public void onError(Throwable t) {
      Metadata metadata = Status.trailersFromThrowable(t);
      boolean overDue = (System.nanoTime() - startNano) > TimeUnit.MILLISECONDS.toNanos(timeoutMs);
      if (metadata != null) {
        String action = metadata.get(KAFKA_ACTION_KEY);
        // 1. act according to kafka-action
        // 2. if action not available fallback to code based action
        if (action != null && !action.isEmpty()) {
          infra.scope().tagged(actionTags(action, tags)).counter(MetricNames.ACTION).inc(1);
          DispatcherResponse.Code code = actionToCode(action);
          future.complete(GrpcResponse.of(Status.fromThrowable(t), code));
        }
      }

      Status status = Status.fromThrowable(t);
      Optional<DispatcherResponse.Code> code = grpcFilter.tryHandleError(t, message, tags);
      if (code.isPresent()) {
        future.complete(GrpcResponse.of(status, code.get()));
      }

      future.complete(GrpcResponse.of(status, Optional.ofNullable(metadata), overDue));
    }

    @Override
    public void onCompleted() {
      future.complete(GrpcResponse.of());
    }

    private DispatcherResponse.Code actionToCode(String action) {
      if (action.equals(RETRY)) {
        return DispatcherResponse.Code.RETRY;
      } else if (action.equals(STASH)) {
        return DispatcherResponse.Code.DLQ;
      } else if (action.equals(SKIP)) {
        return DispatcherResponse.Code.SKIP;
      } else {
        LOGGER.warn("failed to convert kafka-action {}", action);
        return DispatcherResponse.Code.INVALID;
      }
    }
  }

  private static class MetricNames {
    static final String ACTION = "dispatcher.grpc.action";
    static final String TIMEOUT_COUNT = "dispatcher.grpc.timeout-count";
    static final String ADJUSTED_RPC_TIMEOUT = "dispatcher.grpc.adjusted-rpc-timeout";
    static final String CALL = "dispatcher.grpc.call";
    static final String CHANNEL_USAGE = "dispatcher.grpc.channel.usage";
    static final String CHANNEL_SIZE = "dispatcher.grpc.channel.size";
    static final String DISPATCH = "dispatcher.grpc.dispatch";
  }
}
