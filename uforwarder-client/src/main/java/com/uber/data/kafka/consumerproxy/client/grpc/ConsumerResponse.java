package com.uber.data.kafka.consumerproxy.client.grpc;

import com.google.protobuf.Empty;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * ConsumerResponse contains helper functions that allows users to interact with Kafka concepts of
 * (single message) commit / retriable error / non-retriable error.
 *
 * <p>Internally, it translates these to the appropriate gRPC status and response for transmission:
 *
 * <ul>
 *   <li>Commit a single message is converted to gRPC {@code Status.OK}
 *   <li>Retriable Exception is converted to gRPC {@code Status.RESOURCE_EXHAUSTED}
 *   <li>Nonretriable Exception is converted to gRPC {@code Status.PRECONDITION_FAILED}
 * </ul>
 *
 * <p><b>IMPORTANT:</b> you must reply with exactly one KafkaResponse for each message received.
 *
 * <p>Sample Usage (within your {@code ServerCalls.Handler.invoke()} method):
 *
 * <ul>
 *   <li>Commit a message to mark consumption as success
 *       <pre><code>
 *     {@literal @Override}
 *     public void invoke(ByteString data, StreamObserver<Empty> responseObserver) {
 *       // Process the message.
 *       // Processing is successful so commit this message/request.
 *       ConsumerResponse.commit(responseObserver);
 *     }
 *     </code></pre>
 *   <li>Mark message consumption as failed with a retriable error
 *       <pre><code>
 *     {@literal @Override}
 *     public void invoke(ByteString data, StreamObserver<Empty> responseObserver) {
 *       // Process the message.
 *       // Processing failed with a transient / temporary / retriable error
 *       ConsumerResponse.retriableException(responseObserver);
 *     }
 *     </code></pre>
 *   <li>Mark message consumption as failed with a nonretriable error (these messages will be sent
 *       to DLQ)
 *       <pre><code>
 *     {@literal @Override}
 *     public void invoke(ByteString data, StreamObserver<Empty> responseObserver) {
 *       // Process the message.
 *       // Processing failed with a nontransient / non-retriable error
 *       ConsumerResponse.nonRetriableException(responseObserver);
 *     }
 *     </code></pre>
 * </ul>
 */
public final class ConsumerResponse {
  private static final Metadata.Key<String> KAFKA_ACTION_KEY =
      Metadata.Key.of("kafka-action", Metadata.ASCII_STRING_MARSHALLER);
  private static final String RETRY = "Retry";
  private static final String STASH = "Stash";
  private static final String SKIP = "Skip";

  private ConsumerResponse() {}

  /**
   * Marks a single message as successfully processed.
   *
   * @param streamObserver to send the commit response on. <br>
   *     Note: Kafka Consumer Proxy clients treat each message commit individually (as opposed to
   *     Kafka commit up to).
   */
  public static void commit(StreamObserver<Empty> streamObserver) {
    streamObserver.onNext(Empty.getDefaultInstance());
    streamObserver.onCompleted();
  }

  /**
   * Marks a single message for retry because the error is retriable.
   *
   * @param streamObserver to send the retry response on.
   */
  public static void retriableException(StreamObserver<Empty> streamObserver) {
    streamObserver.onError(RetriableException.of());
  }

  /**
   * Marks a single message for DLQ because the error is nonretriable.
   *
   * @param streamObserver to send the DLQ response on.
   */
  public static void nonRetriableException(StreamObserver<Empty> streamObserver) {
    streamObserver.onError(NonRetriableException.of());
  }

  /**
   * Skips a message even if the error is retriable. A nonRetriableError(FAILED_PRECONDITION) status
   * code will be used.
   *
   * @param streamObserver to send the Skip response on.
   */
  public static void dropMessageException(StreamObserver<Empty> streamObserver) {
    dropMessageException(streamObserver, Status.FAILED_PRECONDITION);
  }

  /**
   * Marks a single message for retry because the error is retriable. error code in {@link
   * StatusRuntimeException} will be preserved while sending response
   *
   * @param streamObserver to send the Retry response on.
   * @param status gRPC response status, must be error status
   */
  public static void retriableException(StreamObserver<Empty> streamObserver, Status status) {
    advancedException(streamObserver, status, RETRY);
  }

  /**
   * Marks a single message for DLQ because the error is nonretriable. error code in {@link
   * StatusRuntimeException} will be preserved while sending response
   *
   * @param streamObserver to send the DLQ response on.
   * @param status gRPC response status, must be error status
   */
  public static void nonRetriableException(StreamObserver<Empty> streamObserver, Status status) {
    advancedException(streamObserver, status, STASH);
  }

  /**
   * Skips a message even if the error is retriable. error code in {@link StatusRuntimeException}
   * will be preserved while sending response. If no error is presented(non-failure status), a
   * nonRetriableError(FAILED_PRECONDITION) will be used.
   *
   * @param streamObserver to send the Skip response on.
   * @param status the gRPC response status, can be both error or ok status.
   */
  public static void dropMessageException(StreamObserver<Empty> streamObserver, Status status) {
    if (status.isOk()) {
      status = Status.FAILED_PRECONDITION;
    }
    advancedException(streamObserver, status, SKIP);
  }

  private static void advancedException(
      StreamObserver<Empty> streamObserver, Status status, String action) {
    if (status.isOk()) {
      throw new IllegalArgumentException("must be error status");
    }

    Metadata metadata = new Metadata();
    metadata.put(KAFKA_ACTION_KEY, action);
    streamObserver.onError(new StatusRuntimeException(status, metadata));
  }
}
