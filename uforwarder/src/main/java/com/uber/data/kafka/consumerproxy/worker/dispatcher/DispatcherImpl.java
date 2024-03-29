package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.consumerproxy.common.StructuredTags;
import com.uber.data.kafka.consumerproxy.utils.RetryUtils;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcDispatcher;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcResponse;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.ItemAndJob;
import com.uber.data.kafka.datatransfer.worker.common.Sink;
import com.uber.data.kafka.datatransfer.worker.dispatchers.kafka.KafkaDispatcher;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatcherImpl implements Sink<DispatcherMessage, DispatcherResponse> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DispatcherImpl.class);
  private final CoreInfra infra;
  private final GrpcDispatcher grpcDispatcher;
  private final KafkaDispatcher<byte[], byte[]> dlqProducer;
  private final Optional<KafkaDispatcher<byte[], byte[]>> resqProducer;
  private final AtomicBoolean isRunning;

  // We currently only allow a single grpc dispatcher, we may need to add more gRPC dispatchers
  // to achieve higher # of concurrent connections.
  // For now, Muttley lifted their 250 connection limit in T4521215.
  public DispatcherImpl(
      CoreInfra infra,
      GrpcDispatcher grpcDispatcher,
      KafkaDispatcher<byte[], byte[]> dlqProducer,
      Optional<KafkaDispatcher<byte[], byte[]>> resqProducer) {
    this.infra = infra;
    this.isRunning = new AtomicBoolean(false);
    this.grpcDispatcher = grpcDispatcher;
    this.dlqProducer = dlqProducer;
    this.resqProducer = resqProducer;
  }

  public static DispatcherResponse dispatcherResponseFromGrpcStatus(GrpcResponse resp) {
    if (resp.code().isPresent()) {
      return new DispatcherResponse(resp.code().get());
    }

    switch (resp.status().getCode()) {
        // COMMIT
      case OK:
        // Handle OK responses uniformly as COMMIT.
        // Practically, onCompleted should be invoked on OK instead of onError.
        // gRPC Status OK -> Kafka COMMIT is part of the API contract.
        return new DispatcherResponse(DispatcherResponse.Code.COMMIT);
        // SKIP
      case ALREADY_EXISTS:
        return new DispatcherResponse(DispatcherResponse.Code.SKIP);
        // RETRY
      case RESOURCE_EXHAUSTED:
        // gRPC Status RESOURCE_EXHAUSTED -> Kafka RETRY is part of the API contract.
        return new DispatcherResponse(DispatcherResponse.Code.RETRY);
        // DLQ
      case NOT_FOUND:
      case INVALID_ARGUMENT:
      case FAILED_PRECONDITION:
        // gRPC Status FAILED_PRECONDITION -> Kafka DLQ is part of the API contract.
      case ABORTED:
      case OUT_OF_RANGE:
      case DATA_LOSS:
        return new DispatcherResponse(DispatcherResponse.Code.DLQ);
      case UNAVAILABLE:
        if (resp.isOverDue()) {
          // some reverse proxy (include envoy and muttley) convert timeout error into
          // gRPC code UNAVAILABLE. see
          // https://github.com/grpc/grpc/blob/master/doc/http-grpc-status-mapping.md
          return new DispatcherResponse(DispatcherResponse.Code.BACKOFF);
        } else {
          // Do not backoff, if deadline is not exceeded
          return new DispatcherResponse(DispatcherResponse.Code.INVALID);
        }
      case DEADLINE_EXCEEDED:
        return new DispatcherResponse(DispatcherResponse.Code.BACKOFF);
        // INVALID
      case UNKNOWN: // Log detailed response to debug for UNKNOWN code
        LOGGER.debug("Dispatcher response with code UNKNOWN: {}", resp);
        return new DispatcherResponse(DispatcherResponse.Code.INVALID);
        // INVALID
      case CANCELLED:
      case UNIMPLEMENTED:
      case INTERNAL:
        // gRPC status PERMISSION_DENIED -> returned when provided SPIFFE ids dont have
        // authorization
        // to consume
      case PERMISSION_DENIED:
        // gRPC status UNAUTHETICATED -> returned when provided SPIFFE ids dont match with muttley
        // uri
      case UNAUTHENTICATED:
      default:
        return new DispatcherResponse(DispatcherResponse.Code.INVALID);
    }
  }

  @Override
  public CompletableFuture<DispatcherResponse> submit(ItemAndJob<DispatcherMessage> item) {
    final Job job = item.getJob();
    final DispatcherMessage message = item.getItem();
    // Only emit dispatch by type metric b/c GrpcDispatcher and KafkaDispatcher already emit fine
    // grained success/failure/latency metrics.
    final String rpcUri = job.getRpcDispatcherTask().getUri();
    final String group = job.getKafkaConsumerTask().getConsumerGroup();
    final String topic = job.getKafkaConsumerTask().getTopic();
    final int partition = job.getKafkaConsumerTask().getPartition();
    LOGGER.debug(
        MetricNames.DISPATCH,
        StructuredLogging.dispatcher(message.getType().toString()),
        StructuredLogging.rpcRoutingKey(rpcUri),
        StructuredLogging.kafkaGroup(group),
        StructuredLogging.kafkaTopic(topic),
        StructuredLogging.kafkaPartition(partition),
        StructuredLogging.destination(message.getDestination()));
    infra
        .scope()
        .tagged(
            StructuredTags.builder()
                .setMode(message.getType().toString())
                .setDestination(rpcUri)
                .setKafkaGroup(group)
                .setKafkaTopic(topic)
                .setKafkaPartition(partition)
                .build())
        .counter(MetricNames.DISPATCH)
        .inc(1);
    switch (message.getType()) {
      case GRPC:
        return grpcDispatcher
            .submit(ItemAndJob.of(message.getGrpcMessage(), job))
            // wrap KafkaMessageAction from gRPC response in a Dispatcher response
            // exceptional completions are treated as in-memory retry by the processor,
            // which resends the message to the gRPC endpoint.
            .thenApply(DispatcherImpl::dispatcherResponseFromGrpcStatus)
            .toCompletableFuture();
      case KAFKA:
        KafkaDispatcher<byte[], byte[]> kafkaDispatcher;
        String destTopic = message.getDestination();
        if (RetryUtils.isResqTopic(destTopic, job)) {
          if (!resqProducer.isPresent()) {
            CompletableFuture<DispatcherResponse> future = new CompletableFuture<>();
            future.completeExceptionally(
                new IllegalStateException("resilience queue producer is not present"));
            return future;
          }
          kafkaDispatcher = resqProducer.get();
        } else {
          kafkaDispatcher = dlqProducer;
        }
        return kafkaDispatcher
            .submit(ItemAndJob.of(message.getProducerRecord(), job))
            // wrap KafkaMessageAction from gRPC response in a Dispatcher response
            // exceptional completions are treated as in-memory retry by the processor,
            // which resends the message to the Kafka Producer endpoint.
            .thenApply(r -> new DispatcherResponse(DispatcherResponse.Code.COMMIT))
            .toCompletableFuture();
      default:
        CompletableFuture<DispatcherResponse> future = new CompletableFuture<>();
        future.completeExceptionally(
            new IllegalArgumentException(
                String.format("unsupported message type %s", message.getType())));
        return future;
    }
  }

  @Override
  public void start() {
    LOGGER.info("starting message dispatcher");
    isRunning.set(true);
    LOGGER.info("started message dispatcher");
  }

  @Override
  public boolean isRunning() {
    return isRunning.get();
  }

  @Override
  public void stop() {
    LOGGER.info("stopping message dispatcher");
    try {
      if (grpcDispatcher.isRunning()) {
        grpcDispatcher.stop();
      }
      dlqProducer.stop();
      if (resqProducer.isPresent()) {
        resqProducer.get().stop();
      }
    } catch (Exception e) {
      LOGGER.error("failed to close message dispatcher", e);
      infra.scope().counter(MetricNames.CLOSE_FAILURE).inc(1);
      throw new RuntimeException(e);
    }
    isRunning.set(false);
    infra.scope().counter(MetricNames.CLOSE_SUCCESS).inc(1);
    LOGGER.info("stopped message dispatcher");
  }

  private static class MetricNames {
    static final String DISPATCH = "dispatcher.dispatch";
    static final String CLOSE_SUCCESS = "dispatcher.close.success";
    static final String CLOSE_FAILURE = "dispatcher.close.failure";
  }
}
