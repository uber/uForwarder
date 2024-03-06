package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherImpl;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc.GrpcResponse;
import com.uber.data.kafka.consumerproxy.worker.limiter.InflightLimiter;
import com.uber.m3.tally.NoopScope;
import com.uber.m3.tally.Scope;
import io.grpc.Status;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import net.logstash.logback.argument.StructuredArgument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 * <pre>
 * Works with Async API used in {@link ProcessorImpl}
 * Support head of line blocking resolution by canceling Grpc dispatching
 * </pre>
 *
 * <pre>
 *  when cancel the message, it could either cancel a attempt or cancel the retry future
 *  if there is inflight attempt, do deep cancellation by cancel the rpc call to avoid memory leak
 *  otherwise cancel the retry future to avoid block by back-off
 *  both case eventually should be handled by fallback policy
 * </pre>
 *
 * <pre>
 *  It ensures after cancel attempts stopped immediately and no future attempts will be made
 *  However it could be still possible grpc OK and cancel happen at the same time, which leads to double ack of message
 * </pre>
 */
public class MessageStub {
  private static final Logger LOGGER = LoggerFactory.getLogger(MessageStub.class);
  private static final int MAX_DEBUG_EVENTS = 10;
  // excessive debug event will be evicted from head of queue
  private final Collection<DebugEvent> debugEvents =
      Collections.synchronizedCollection(EvictingQueue.create(MAX_DEBUG_EVENTS));
  private final AtomicReference<DispatcherResponse.Code> cancelCode = new AtomicReference(null);
  private final Scope scope;
  private final StructuredArgument[] jobTags;
  private final AtomicInteger attemptCount = new AtomicInteger(0);
  private volatile boolean closed = false;
  @Nullable private volatile CompletableFuture retryFuture = null;
  @Nullable private volatile Cancelable currentAttempt = null;
  @Nullable private volatile CompletableFuture<InflightLimiter.Permit> futurePermit = null;

  public MessageStub(StructuredArgument[] jobTags, Scope jobScope) {
    this.jobTags = jobTags;
    this.scope = jobScope;
  }

  public MessageStub() {
    this(new StructuredArgument[] {}, new NoopScope());
  }

  /**
   * Supplies with retry future generated from {@link net.jodah.failsafe.Failsafe} retry policy
   *
   * @param future
   * @return decorated future
   */
  public synchronized <T> CompletableFuture<T> withRetryFuture(CompletableFuture<T> future) {
    if (retryFuture != null) {
      throw new IllegalStateException("Start new retry without finishing previous one");
    }

    // if not canceled
    retryFuture = future;
    return retryFuture;
  }

  public synchronized CompletableFuture<InflightLimiter.Permit> withFuturePermit(
      CompletableFuture<InflightLimiter.Permit> future) {
    if (futurePermit != null) {
      throw new IllegalStateException("Acquire permit without finishing previous one");
    }

    if (tryComplete(future, DebugStatus.PERMIT_PASSIVE_CANCELED)) {
      return future;
    }

    futurePermit = future;
    return futurePermit.whenComplete((r, t) -> futurePermit = null);
  }

  private boolean tryComplete(CompletableFuture future, DebugStatus debugStatus) {
    if (future.isDone()) {
      return true;
    }

    if (closed) {
      // ack received
      return true;
    }

    if (cancelCode.get() != null) {
      logDebugStatus(debugStatus);
      // complete future if stub already canceled
      future.cancel(false);
      return true;
    }

    return false;
  }

  /**
   * Creates new attempt
   *
   * <pre>should be closed with Attempt.complete()</pre>
   *
   * @return attempt
   * @throws IllegalStateException when new attempt made before previous attempt finish
   */
  public synchronized Attempt newAttempt() {
    if (currentAttempt != null) {
      throw new IllegalStateException("Start new attempt without finishing previous one");
    }

    return new CancelableAttempt();
  }

  /**
   * Cancels message processing with reason
   *
   * <pre>
   *   Cancel works at first time
   * </pre>
   *
   * @param code the cancel code
   * @return boolean indicates if cancel works
   */
  public synchronized boolean cancel(DispatcherResponse.Code code) {
    if (closed) {
      // ack received
      return false;
    }

    if (cancelCode.compareAndSet(null, code)) {
      final CompletableFuture finalFuturePermit = futurePermit;
      final Cancelable finalCurrentAttempt = currentAttempt;
      if (finalFuturePermit != null) {
        // complete the future permit
        finalFuturePermit.cancel(false);
        logDebugStatus(DebugStatus.PERMIT_CANCEL);
      } else if (finalCurrentAttempt != null) {
        // cancel happens during an attempt, cancel the attempt
        // canceled attempt should skip retry with grpc Cancelled state
        finalCurrentAttempt.cancel();
        logDebugStatus(DebugStatus.ATTEMPT_CANCEL);
      } else {
        if (retryFuture != null && attemptCount.get() > 0) {
          // cancel the retry future, attempt if scheduled in failsafe due to backoff
          // should be cancel as well, and fallback to fallback policy with CancellationException
          logDebugStatus(DebugStatus.RETRY_CANCEL);
          retryFuture.cancel(false);
        } else {
          logDebugStatus(DebugStatus.PASSIVE_CANCEL);
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Gets cancel code
   *
   * @return optional cancel code
   */
  public Optional<DispatcherResponse.Code> cancelCode() {
    return Optional.ofNullable(cancelCode.get());
  }

  public List<String> getDebugInfo() {
    return debugEvents.stream().map(DebugEvent::toString).collect(Collectors.toList());
  }

  @VisibleForTesting
  protected @Nullable Cancelable getCurrentAttempt() {
    return currentAttempt;
  }

  @VisibleForTesting
  protected void logDebugStatus(DebugStatus debugStatus) {
    debugEvents.add(new DebugEvent(debugStatus));
    scope.counter(String.format(MetricNames.PROCESSOR_STUB_TEMPLATE, debugStatus.name)).inc(1);
  }

  /** Attempt indicates a dispatch action */
  public interface Attempt {
    /**
     * if the attempt already canceled
     *
     * @return
     */
    boolean isCanceled();
    /**
     * reactor invoked exactly once when cancel
     *
     * <pre>
     *   if canceled before onCancel, reactor invoked immediately
     *   else, reactor invoked on cancel
     * </pre>
     *
     * @param reactor
     */
    void onCancel(Runnable reactor);

    /**
     * inject result handling between promise and downstream stage
     *
     * @return decorated future
     */
    CompletionStage<GrpcResponse> complete(CompletionStage<GrpcResponse> stage);
  }

  private interface Cancelable {
    void cancel();
  }

  /** A dispatch attempt, each message should have at most 1 inflight attempt */
  private class CancelableAttempt implements Attempt, Cancelable {
    private final Set<Runnable> reactors = ConcurrentHashMap.newKeySet();

    private CancelableAttempt() {
      currentAttempt = this;
    }

    @Override
    public boolean isCanceled() {
      if (cancelCode.get() != null) {
        // cancelled before making GRPC call
        logDebugStatus(DebugStatus.ATTEMPT_PASSIVE_CANCELED);
        return true;
      }
      return false;
    }

    @Override
    public void onCancel(Runnable reactor) {
      Runnable safeReactor =
          () -> {
            try {
              reactor.run();
            } catch (Exception e) {
              scope.counter(MetricNames.DISPATCHER_CANCEL_FAILURE).inc(1);
              LOGGER.error("Failed to cancel attempt", jobTags, e);
            }
          };

      if (cancelCode.get() != null) {
        safeReactor.run();
      } else {
        reactors.add(safeReactor);
      }
    }

    @Override
    public CompletionStage<GrpcResponse> complete(CompletionStage<GrpcResponse> stage) {
      return stage
          .thenApply(
              r -> {
                synchronized (MessageStub.this) {
                  if (closed) {
                    // Attempt should not be made after close
                    // Simply skip it by return already_exists code
                    LOGGER.warn("Attempt complete after stub close", jobTags);
                    return GrpcResponse.of(Status.ALREADY_EXISTS);
                  }
                  DispatcherResponse resp = DispatcherImpl.dispatcherResponseFromGrpcStatus(r);
                  GrpcResponse result = r;
                  switch (resp.getCode()) {
                    case SKIP:
                    case COMMIT:
                      closed = true;
                      logDebugStatus(DebugStatus.CLOSED);
                      break;
                    default:
                      if (cancelCode.get() != null) {
                        logDebugStatus(DebugStatus.ATTEMPT_CANCELED);
                        result = GrpcResponse.of(Status.CANCELLED, cancelCode.get());
                      }
                  }

                  return result;
                }
              })
          .whenComplete(
              (r, t) -> {
                attemptCount.incrementAndGet();
                currentAttempt = null;
              });
    }

    @Override
    public void cancel() {
      for (Runnable r : reactors) {
        r.run();
      }
    }
  }

  @VisibleForTesting
  protected enum DebugStatus {
    RETRY_CANCEL("retry-cancel"),
    PERMIT_CANCEL("permit-cancel"),
    ATTEMPT_CANCEL("attempt-cancel"),
    PASSIVE_CANCEL("passive-cancel"),
    RETRY_CANCELLED("retry-canceled"),
    PERMIT_CANCELLED("permit-canceled"),
    ATTEMPT_CANCELED("attempt-canceled"),
    ATTEMPT_PASSIVE_CANCELED("attempt-passive-canceled"),
    PERMIT_PASSIVE_CANCELED("permit-passive-canceled"),
    CLOSED("closed");

    final String name;

    DebugStatus(String name) {
      this.name = name;
    }
  }

  private static class DebugEvent {
    private static final String TEMPLATE = "%d %s";
    private final long timestamp;
    private final DebugStatus status;

    private DebugEvent(DebugStatus status) {
      this.timestamp = System.nanoTime();
      this.status = status;
    }

    @Override
    public String toString() {
      return String.format(TEMPLATE, timestamp, status);
    }
  }

  private static class MetricNames {
    static final String PROCESSOR_STUB_TEMPLATE = "processor.stub.%s";
    static final String DISPATCHER_CANCEL_FAILURE = "dispatcher.cancel.failure";
  }
}
