package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.worker.limiter.InflightLimiter;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import java.util.concurrent.CompletableFuture;

/** limiter of dispatching messages */
public interface OutboundMessageLimiter extends MetricSource {

  /**
   * Acquires a permit asynchronously.
   *
   * @param processorMessage the processor message
   * @return non -empty optional if acquired successfully
   */
  CompletableFuture<InflightLimiter.Permit> acquirePermitAsync(ProcessorMessage processorMessage);

  /**
   * Acquires a permit synchronously.
   *
   * @param processorMessage the processor message
   * @return permit
   */
  InflightLimiter.Permit acquirePermit(ProcessorMessage processorMessage);

  /**
   * Updates limit asynchronous.
   *
   * @param limit the limit
   */
  void updateLimit(int limit);

  /** Closes the limiter. Blocking requests will be unblocked */
  void close();

  /**
   * Initializes a job
   *
   * @param job the job
   */
  void init(Job job);

  /**
   * Cancels a job
   *
   * @param job the job
   */
  void cancel(Job job);

  /** Cancels all jobs */
  void cancelAll();

  /**
   * Checks if limiter contains the job
   *
   * @param job the job
   * @return the boolean
   */
  boolean contains(Job job);

  /**
   * Gets metrics of the limiter
   *
   * @return
   */
  Stats getStats();

  /** Factory of OutboundMessageLimiter */
  interface Builder {

    /**
     * Builds outbound message limiter.
     *
     * @return the outbound message limiter
     */
    OutboundMessageLimiter build(Job job);
  }

  /** Statistic of the limiter */
  interface Stats {

    /**
     * Gets if actual inflight is close to limit
     *
     * @return the boolean
     */
    boolean isCloseToFull();

    /**
     * Gets one min average usage
     *
     * @return the double
     */
    double oneMinAverageUsage();
  }
}
