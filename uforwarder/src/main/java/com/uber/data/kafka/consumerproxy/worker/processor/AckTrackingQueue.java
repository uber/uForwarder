package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.datatransfer.worker.common.MetricSource;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

/**
 * AckTrackingQueue tracks the received/nacked/acked status of a range of offsets for a
 * topic-partition.
 *
 * <ol>
 *   <li>received means a message is received and has not been processed by the recipient service
 *       yet
 *   <li>nacked means a message was failed to processed by the recipient service and it should be
 *       produced to the retry topic or dlq topic
 *   <li>acked means a message was successfully processed by the recipient service or it was
 *       produced to the retry topic or dlq topic
 * </ol>
 *
 * <p>When using correctly,
 *
 * <ol>
 *   <li>each individual message can be received, acked or nacked separately
 *   <li>there is no data loss
 *   <li>message fetching cannot go too far, the number of unacked message must under capacity
 * </ol>
 *
 * To use AckTrackingQueue,
 *
 * <ol>
 *   <li>create an AckTrackingQueue for each topic-partition
 *   <li>invoke {@code AckTrackingQueue#receive} to mark an offset as received
 *   <li>invoke {@code AckTrackingQueue#nack} to mark an offset as nacked -- the corresponding
 *       message should be produced to the retry queue topic or dlq topic
 *   <li>invoke {@code AckTrackingQueue#nack} to mark an offset as acked -- the corresponding
 *       message has already be processed correctly by the recipient service or it has already been
 *       produced to the retry queue topic or dlq topic
 *   <li>invoke {@code AckTrackingQueue#markAsNotInUse} to mark the AckTrackingQueue has been
 *       discard, it is used when a job is canceled
 * </ol>
 */
public interface AckTrackingQueue extends MetricSource {

  /** CANNOT_ACK if the offset cannot be acked -- out of the tracking range. */
  long CANNOT_ACK = -1;
  /** DUPLICATED_ACK if the offset was acked before. */
  long DUPLICATED_ACK = -2;
  /** IN_MEMORY_ACK_ONLY if the caller should not commit to the server side. */
  long IN_MEMORY_ACK_ONLY = -3;

  /**
   * marks an offset received by AckTrackingQueue with key
   *
   * @param offset the offset
   * @param key the key
   * @throws InterruptedException the interrupted exception
   */
  void receive(long offset, String key) throws InterruptedException;

  /**
   * marks an offset received by AckTrackingQueue
   *
   * @param offset the offset
   * @throws InterruptedException the interrupted exception
   */
  void receive(long offset) throws InterruptedException;

  /**
   * acks an offset, which means a message was successfully processed by the recipient service or it
   * was produced to the retry topic or dlq topic
   *
   * @param offset the offset to be acked. following kafka convention, this offset should be (the
   *     offset of the message to be acked + 1)
   * @return
   *     <ol>
   *       <li>CANNOT_ACK if the offset cannot be acked -- out of the tracking range
   *       <li>DUPLICATED_ACK if the offset was acked before
   *       <li>IN_MEMORY_ACK_ONLY if the caller should not commit to the server side
   *       <li>an offset for the caller to commit to the server
   *     </ol>
   *
   * @throws InterruptedException when the current thread is trying to ack the offset, it may be
   *     interrupted by another thread. If the caller gets this exception, it should give up.
   */
  long ack(long offset) throws InterruptedException;

  /**
   * nacks an offset, which means a message was failed to processed by the recipient service and it
   * should be produced to the retry topic or dlq topic
   *
   * @param offset the offset to be nacked. following kafka convention, this offset should be (the
   *     offset of the message to be acked + 1)
   * @return
   *     <ol>
   *       <li>true if the nack succeeds (i.e., the given offset can be nacked)
   *       <li>false if it fails (i.e., the given offset doesn't exist or already nacked)
   *     </ol>
   *
   * @throws InterruptedException when the current thread is trying to nack the offset, it may be
   *     interrupted by another thread. If the caller gets this exception, it should give up.
   */
  boolean nack(long offset) throws InterruptedException;

  /** Mark as not in use. */
  void markAsNotInUse();

  /** initialize cancel of offset */
  boolean cancel(long offset) throws InterruptedException;

  /**
   * The status of each offset.
   *
   * <pre>
   *   start state: UNSET
   *   end state: ACKED
   *   legit state transitions:
   *    UNSET -> NACKED, CANCELED, ACKED
   *    NACKED -> CANCELED, ACKED
   *    CANCELED -> ACKED
   * </pre>
   */
  enum AckStatus {
    /** Unset ack status. */
    UNSET,
    /** Nacked ack status. */
    NACKED,
    /** Cancel started */
    CANCELED,
    /** Acked ack status. */
    ACKED
  }

  /**
   * Gets state of the queue
   *
   * @return the state
   */
  State getState();

  /**
   * Gets TopicPartition.
   *
   * @return the TopicPartition
   */
  TopicPartition getTopicPartition();

  /**
   * Adds a reactor.
   *
   * @param reactor the reactor
   */
  void addReactor(Reactor reactor);

  /** State of the {@link AckTrackingQueue} */
  interface State {
    /**
     * Gets capacity of the queue, it's the storage space for all offsets
     *
     * @return the int
     */
    int capacity();

    /**
     * Gets statistics
     *
     * @return
     */
    Stats stats();
    /**
     * Get head of line offset. -1 if queue is empty
     *
     * @return the long
     */
    long headOffset();

    /**
     * Get tail of line offset. -1 if queue is empty
     *
     * @return the long
     */
    long tailOffset();

    /**
     * Get first cancelable offset from head of line. -1 if no offset is cancellable
     *
     * @return the long
     */
    long lowestCancelableOffset();

    /**
     * Get highest offset acked Note, acked offset equals to actual message offset + 1
     *
     * @return
     */
    long highestAckedOffset();

    /**
     * Gets statistics by key
     *
     * @return
     */
    Map<String, Stats> keyStats();

    /**
     * Gets load or usage of the queue. usage is defined by percentage of queue used of its total
     * capacity higher the value indicates higher risk of head of line blocking s
     *
     * @param excludeCanceled exclude cancelled offset
     * @return
     */
    double loadFactor(boolean excludeCanceled);
  }

  /** Statistics */
  interface Stats {
    /**
     * Gets actual number of offsets in the queue
     *
     * @return the int
     */
    int size();

    /**
     * Gets count of offsets acked in memory, doesn't include already committed offsets
     *
     * @return
     */
    int acked();

    /**
     * Gets count of offsets canceled in memory, doesn't include already committed offsets
     *
     * @return
     */
    int canceled();
  }

  /** Reactor of {@link AckTrackingQueue} */
  interface Reactor {

    /** Invoked when {@link AckTrackingQueue} reset its internal state */
    void onReset();
  }

  /** Metric names. */
  class MetricNames {
    static final String STATUS_CHANGE_WAITING_TIME = "tracking-queue.status-change.waiting-time";
    static final String HIGHEST_OFFSET_ACKED = "tracking-queue.highest-offset.acked";
    static final String HIGHEST_OFFSET_COMMITTED = "tracking-queue.highest-offset.committed";
    static final String HIGHEST_OFFSET_RECEIVED = "tracking-queue.highest-offset.received";
    static final String IN_MEMORY_UNCOMMITTED = "tracking-queue.in-memory.uncommitted";
    static final String IN_MEMORY_CAPACITY = "tracking-queue.in-memory.capacity";
    static final String OFFSET_RECEIVED_GAP = "tracking-queue.offset.received.gap";
    static final String LOAD_FACTOR_EXCLUSIVE = "tracking-queue.blocking.load-factor-exclusive";
    static final String LOAD_FACTOR = "tracking-queue.blocking.load-factor";
    static final String ACKED_COUNT = "tracking-queue.blocking.acked";
    static final String CANCELED_COUNT = "tracking-queue.blocking.cancelled";
  }
}
