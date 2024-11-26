package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.uber.data.kafka.consumer.DLQMetadata;
import com.uber.data.kafka.consumerproxy.utils.RetryUtils;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherMessage;
import com.uber.data.kafka.consumerproxy.worker.limiter.InflightLimiter;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.worker.common.TracedConsumerRecord;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.log.Fields;
import io.opentracing.tag.Tags;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/**
 * A ProcessorMessage is converted from is converted from a consumer record, and can be converted to
 * a {@code DispatcherMessage}. It carries information necessary for {@link ProcessorImpl} to track
 * processing stages of the message.
 */
public final class ProcessorMessage {
  // This is the header that stores the name of the original cluster that the data was first
  // produced to
  // per
  // https://docs.google.com/document/d/1Qt-j22og8GVX3MjC0DtwpLOxibiFLfMb2Lgx7K7qk34/edit#bookmark=id.33kf5skhebf7.
  private static final String PRODUCER_CLUSTER_HEADER_KEY = "original_cluster";
  private static final String DISPATCH_OPERATION_NAME = "dispatch";

  // User payload
  private final byte[] key;
  private final byte[] value;
  private final Headers kafkaHeaders;

  // physicalXXX is the kafka metadata that this actually corresponds to.
  private final String physicalTopic;
  private final String physicalCluster;
  private final int physicalPartition;
  private final long physicalOffset;
  private final long physicalTimestamp;

  // logicalXXX is the logical kafka metadata that this data corresponds to.
  // For retryQ/DLQ data, this corresponds to the original topic non retryQ/DLQ topic.
  private final String logicalGroup;
  private final String logicalTopic;
  private final int logicalPartition;
  private final long logicalOffset;
  private final long logicalTimestamp;
  private final AtomicLong retryCount;
  private final AtomicLong dispatchAttempt;
  private final AtomicLong timeoutCount;

  // cleanup subroutine for that wraps state that needs to be cleared.
  private final BiConsumer<DispatcherResponseAndOffset, Throwable> cleanup;

  // offsetToCommit is the offset to commit
  private final AtomicLong offsetToCommit;
  // shouldDispatch determines whether this message should be dispatched to user.
  private final AtomicBoolean shouldDispatch;

  // tracing
  private final Span span;
  // cancel processing
  private final MessageStub stub;

  // inflight permit
  private Optional<InflightLimiter.Permit> permit = Optional.empty();

  @VisibleForTesting
  ProcessorMessage(
      byte[] key,
      byte[] value,
      Headers kafkaHeaders,
      String physicalTopic,
      String physicalCluster,
      int physicalPartition,
      long physicalOffset,
      long physicalTimestamp,
      String logicalGroup,
      String logicalTopic,
      int logicalPartition,
      long logicalOffset,
      long logicalTimestamp,
      long retryCount,
      long timeoutCount,
      Optional<Span> parentSpan,
      CoreInfra infra,
      MessageStub stub) {
    this.key = key;
    this.value = value;
    this.kafkaHeaders = kafkaHeaders;
    this.physicalTopic = physicalTopic;
    this.physicalCluster = physicalCluster;
    this.physicalPartition = physicalPartition;
    this.physicalOffset = physicalOffset;
    this.physicalTimestamp = physicalTimestamp;
    this.logicalGroup = logicalGroup;
    this.logicalTopic = logicalTopic;
    this.logicalPartition = logicalPartition;
    this.logicalOffset = logicalOffset;
    this.logicalTimestamp = logicalTimestamp;
    this.retryCount = new AtomicLong(retryCount);
    this.dispatchAttempt = new AtomicLong(0);
    this.timeoutCount = new AtomicLong(timeoutCount);
    this.offsetToCommit = new AtomicLong(-1);
    this.shouldDispatch = new AtomicBoolean(true);
    this.span =
        createDispatchSpan(
            infra.tracer(),
            parentSpan,
            logicalGroup,
            physicalTopic,
            physicalPartition,
            physicalOffset);
    this.stub = stub;
    this.cleanup =
        (r, t) -> {
          if (r != null) {
            span.log(ImmutableMap.of(Fields.MESSAGE, r));
          }
          if (t != null) {
            span.log(ImmutableMap.of(Fields.ERROR_OBJECT, t));
          }

          span.finish();
        };
  }

  public int getValueByteSize() {
    return value == null ? 0 : value.length;
  }

  /**
   * Attempt count: the max number of delivery attempts made after a message is fetched from a
   * queue.
   *
   * <p>In case of gRPC Dispatch, Attempt count will be incremented all gRPC status
   */
  public void increaseAttemptCount() {
    dispatchAttempt.incrementAndGet();
  }

  /**
   * Retry count: the minimum number of delivery attempts made in the whole life cycle of a message.
   *
   * <p>In case of gRPC Dispatch, Attempt count will be incremented only for gRPC status
   * RESOURCE_EXHAUSTED and DATA_LOSS
   */
  public void increaseRetryCount() {
    retryCount.incrementAndGet();
  }

  public void increaseTimeoutCount() {
    timeoutCount.incrementAndGet();
  }

  public TopicPartitionOffset getPhysicalMetadata() {
    return new TopicPartitionOffset(physicalTopic, physicalPartition, physicalOffset);
  }

  public static ProcessorMessage of(
      ConsumerRecord<byte[], byte[]> consumerRecord, Job job, CoreInfra infra, MessageStub stub)
      throws Exception {
    Optional<Span> span;
    if (consumerRecord instanceof TracedConsumerRecord) {
      span = ((TracedConsumerRecord) consumerRecord).span();
    } else {
      span = Optional.empty();
    }

    if (RetryUtils.isRetryTopic(consumerRecord.topic(), job)
        || RetryUtils.isDLQTopic(consumerRecord.topic(), job)
        || RetryUtils.isResqTopic(consumerRecord.topic(), job)) {

      DLQMetadata dlqMetadata = DLQMetadata.parseFrom(consumerRecord.key());
      return new ProcessorMessage(
          dlqMetadata.getData().toByteArray(),
          consumerRecord.value(),
          consumerRecord.headers(),
          consumerRecord.topic(),
          job.getKafkaConsumerTask().getCluster(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.timestamp(),
          job.getKafkaConsumerTask().getConsumerGroup(),
          dlqMetadata.getTopic(),
          dlqMetadata.getPartition(),
          dlqMetadata.getOffset(),
          dlqMetadata.getTimestampNs(),
          dlqMetadata.getRetryCount(),
          dlqMetadata.getTimeoutCount(),
          span,
          infra,
          stub);
    }

    return new ProcessorMessage(
        consumerRecord.key(),
        consumerRecord.value(),
        consumerRecord.headers(),
        consumerRecord.topic(),
        job.getKafkaConsumerTask().getCluster(),
        consumerRecord.partition(),
        consumerRecord.offset(),
        consumerRecord.timestamp(),
        job.getKafkaConsumerTask().getConsumerGroup(),
        consumerRecord.topic(),
        consumerRecord.partition(),
        consumerRecord.offset(),
        consumerRecord.timestamp(),
        0,
        0,
        span,
        infra,
        stub);
  }

  @VisibleForTesting
  long getRetryCount() {
    return retryCount.get();
  }

  @VisibleForTesting
  long getDispatchAttempt() {
    return dispatchAttempt.get();
  }

  long getTimeoutCount() {
    return timeoutCount.get();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProcessorMessage that = (ProcessorMessage) o;
    return physicalPartition == that.physicalPartition
        && physicalOffset == that.physicalOffset
        && physicalTimestamp == that.physicalTimestamp
        && logicalPartition == that.logicalPartition
        && logicalOffset == that.logicalOffset
        && logicalTimestamp == that.logicalTimestamp
        && Arrays.equals(key, that.key)
        && Arrays.equals(value, that.value)
        && physicalTopic.equals(that.physicalTopic)
        && logicalGroup.equals(that.logicalGroup)
        && logicalTopic.equals(that.logicalTopic)
        && getRetryCount() == that.getRetryCount()
        && getDispatchAttempt() == that.getDispatchAttempt()
        && getTimeoutCount() == that.getTimeoutCount();
  }

  @Override
  public int hashCode() {
    int result =
        Objects.hash(
            physicalTopic,
            physicalPartition,
            physicalOffset,
            physicalTimestamp,
            logicalGroup,
            logicalTopic,
            logicalPartition,
            logicalOffset,
            logicalTimestamp,
            getRetryCount(),
            getDispatchAttempt(),
            getTimeoutCount());
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  public DispatcherMessage getGrpcDispatcherMessage(String rpcUri) {
    return new DispatcherMessage(
        DispatcherMessage.Type.GRPC,
        rpcUri,
        key,
        value,
        kafkaHeaders,
        logicalGroup,
        logicalTopic,
        logicalPartition,
        logicalOffset,
        stub,
        physicalTopic,
        physicalCluster,
        physicalPartition,
        physicalOffset,
        retryCount.get(),
        dispatchAttempt.get(),
        timeoutCount.get());
  }

  public DispatcherMessage getKafkaDispatcherMessage(String topic) {
    DLQMetadata.Builder dlqMetadataBuilder =
        DLQMetadata.newBuilder()
            .setTopic(logicalTopic)
            .setPartition(logicalPartition)
            .setOffset(logicalOffset)
            .setTimestampNs(logicalTimestamp)
            .setRetryCount(retryCount.get())
            .setTimeoutCount(timeoutCount.get());
    if (key != null) {
      dlqMetadataBuilder.setData(ByteString.copyFrom(key));
    }
    return new DispatcherMessage(
        DispatcherMessage.Type.KAFKA,
        topic,
        dlqMetadataBuilder.build().toByteArray(),
        value,
        kafkaHeaders,
        logicalGroup,
        logicalTopic,
        logicalPartition,
        logicalOffset,
        stub,
        physicalTopic,
        physicalCluster,
        physicalPartition,
        physicalOffset,
        retryCount.get(),
        dispatchAttempt.get(),
        timeoutCount.get());
  }

  /** Close is invoked to cleanup tracer state from async message dispatch. */
  public void close(DispatcherResponseAndOffset response, Throwable t) {
    cleanup.accept(response, t);
  }

  /**
   * Extract parent span from message header, builds a new span(if parent span exists), and
   * activates it, then inject new span into message header. The tracer scope that needs to be
   * closed is passed to cleanup, which should be closed after processing of this message is
   * complete.
   */
  private static Span createDispatchSpan(
      Tracer tracer,
      Optional<Span> parentSpan,
      String group,
      String topic,
      int partition,
      long offset) {
    Tracer.SpanBuilder spanBuilder =
        tracer
            .buildSpan(DISPATCH_OPERATION_NAME)
            .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CONSUMER)
            .withTag(TracedConsumerRecord.TAG_CONSUMER_GROUP, group)
            .withTag(TracedConsumerRecord.TAG_TOPIC, topic)
            .withTag("partition", partition)
            .withTag("offset", offset);
    if (parentSpan.isPresent()) {
      spanBuilder = spanBuilder.asChildOf(parentSpan.get());
    }
    return spanBuilder.start();
  }

  public Span getSpan() {
    return span;
  }

  public boolean shouldDispatch() {
    return this.shouldDispatch.get();
  }

  public void setShouldDispatch(boolean shouldDispatch) {
    this.shouldDispatch.set(shouldDispatch);
  }

  public void setOffsetToCommit(long offsetToCommit) {
    this.offsetToCommit.set(offsetToCommit);
  }

  public long getOffsetToCommit() {
    return this.offsetToCommit.get();
  }

  // TODO: remove once zone isolation mode support zone distribution
  // (https://t3.uberinternal.com/browse/KAFEP-3602)
  public String getPhysicalCluster() {
    return physicalCluster;
  }

  public String getProducerCluster() {
    @Nullable Header header = this.kafkaHeaders.lastHeader(PRODUCER_CLUSTER_HEADER_KEY);
    return header == null
        ? ""
        : new String(
            this.kafkaHeaders.lastHeader(PRODUCER_CLUSTER_HEADER_KEY).value(),
            StandardCharsets.UTF_8);
  }

  /**
   * Gets Kafka headers
   *
   * @return
   */
  public Headers getHeaders() {
    return kafkaHeaders;
  }

  public void setPermit(InflightLimiter.Permit permit) {
    this.permit = Optional.of(permit);
  }

  public Optional<InflightLimiter.Permit> getPermit() {
    return this.permit;
  }

  /**
   * Gets stub
   *
   * @return the stub
   */
  public MessageStub getStub() {
    return stub;
  }

  @FunctionalInterface
  private interface ThrowingRunnable<E extends Exception> {
    void run() throws E;
  }
}
