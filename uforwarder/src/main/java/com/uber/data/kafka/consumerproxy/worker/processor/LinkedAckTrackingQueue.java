package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.m3.tally.Scope;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LinkedAckTrackingQueue implements {@link AckTrackingQueue} in Hash and LinkedList
 *
 * <pre>
 * LinkedAckTrackingQueue assume messages received is in increasing order. If out-of-order offset received, this
 * implementation will ignore the offset
 * </pre>
 *
 * ThreadSafe
 */
final class LinkedAckTrackingQueue extends AbstractAckTrackingQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(LinkedAckTrackingQueue.class);

  final LinkedHashMap<Long, OffsetStatus> offsetStatusMap;

  /** offsets can be canceled */
  final LinkedHashSet<Long> cancelableOffsets;

  /** highest message offset received from kafka server */
  volatile long highestReceivedOffset = INITIAL_OFFSET;

  /** highest message offset acked */
  volatile long highestAckedOffset = INITIAL_OFFSET;

  /** highest message offset committed */
  volatile long highestCommittedOffset = INITIAL_OFFSET;

  /** lowest message offset in queue */
  volatile long headOffset = INITIAL_OFFSET;

  LinkedAckTrackingQueue(Job job, int capacity, Scope jobScope) {
    super(job, capacity, jobScope, LOGGER);
    offsetStatusMap = new LinkedHashMap<>();
    cancelableOffsets = new LinkedHashSet<>();
  }

  /**
   * LinkedAckTrackingQueue assume messages received is in increasing order If not, this
   * implementation will ignore the offset
   *
   * <ol>
   *   <li>this method blocks when the offset to receive goes to far
   *   <li>an {@code AckTrackingQueue#ack} unblocks the blocked {@code AckTrackingQueue#receive}
   *       when it makes (number of un-committed messages < capacity)
   *   <li>an {@code AckTrackingQueue#markAsNotInUse} unblocks the blocked {@code
   *       AckTrackingQueue#receive}
   * </ol>
   *
   * @param offset the offset
   * @throws InterruptedException
   */
  @Override
  public synchronized void receive(long offset, Optional<String> key) throws InterruptedException {
    lock.lock();
    try {
      // receive offsets in order
      if (highestReceivedOffset < offset) {
        scope.gauge(AckTrackingQueue.MetricNames.HIGHEST_OFFSET_RECEIVED).update(offset);
        if (highestReceivedOffset == INITIAL_OFFSET) {
          headOffset = offset;
          highestCommittedOffset = offset;
          highestAckedOffset = offset;
        }
        // the first time to receive
        if (waitForNotFull(offset)) {
          offsetStatusMap.put(offset, new OffsetStatus(offset, key));
          cancelableOffsets.add(offset);
          highestReceivedOffset = offset;
        }
        updateState();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long ack(long offset) {
    if (!validateOffset(offset)) {
      return CANNOT_ACK;
    }

    scope.gauge(MetricNames.IN_MEMORY_CAPACITY).update(capacity);
    lock.lock();
    try {
      // condition 3: has never received the offset before
      final OffsetStatus curStatus = offsetStatusMap.get(offset - 1);
      if (curStatus == null) {
        return CANNOT_ACK;
      }

      // condition 4: the offset has been acked before
      if (curStatus.ackStatus == AckStatus.ACKED) {
        return DUPLICATED_ACK;
      }

      updateOffsetStatus(curStatus, AckStatus.ACKED);
      if (curStatus.ackStatus != AckStatus.CANCELED) {
        cancelableOffsets.remove(curStatus.offset);
      }
      // if head of the queue is in acked status, the highest acked offset need to commit to broker
      // otherwise, just ack in memory
      Iterator<OffsetStatus> iter = offsetStatusMap.values().iterator();
      OffsetStatus offsetStatus = iter.next();
      long ret;
      if (offsetStatus.ackStatus == AckStatus.ACKED) {
        // purge all acked offsets from head of queue and update highestCommittedOffset
        while (offsetStatus != null && offsetStatus.ackStatus == AckStatus.ACKED) {
          highestCommittedOffset = offsetStatus.offset + 1;
          iter.remove();
          offsetStatus.close();
          offsetStatus = iter.hasNext() ? iter.next() : null;
        }
        // with empty queue make value of headOffset consist with ArrayTrackingQueue
        headOffset = offsetStatus != null ? offsetStatus.offset : highestCommittedOffset;
        notFull.signalAll();
        scope
            .gauge(AckTrackingQueue.MetricNames.HIGHEST_OFFSET_COMMITTED)
            .update(highestCommittedOffset);
        scope
            .gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED)
            .update(offsetStatusMap.size());
        ret = highestCommittedOffset;
      } else {
        // ack in memory
        if (highestAckedOffset < offset) {
          highestAckedOffset = offset;
          scope.gauge(AckTrackingQueue.MetricNames.HIGHEST_OFFSET_ACKED).update(offset);
          scope
              .gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED)
              .update(offsetStatusMap.size());
        }
        ret = IN_MEMORY_ACK_ONLY;
      }
      updateState();
      return ret;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean nack(long offset) {
    if (!validateOffset(offset)) {
      return false;
    }

    lock.lock();
    try {
      // condition 3: has never received the offset before
      final OffsetStatus curStatus = offsetStatusMap.get(offset - 1);
      if (curStatus == null) {
        return false;
      }

      // condition 4: the offset has been nacked before
      if (curStatus.ackStatus != AckStatus.UNSET) {
        return false;
      }

      updateOffsetStatus(curStatus, AckStatus.NACKED);
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean cancel(long offset) throws InterruptedException {
    if (!validateOffset(offset)) {
      return false;
    }

    lock.lock();
    try {
      // condition 3: has never received the offset before
      final OffsetStatus curStatus = offsetStatusMap.get(offset - 1);
      if (curStatus == null) {
        return false;
      }

      // condition 4: the offset has been acked or canceled before
      if (curStatus.ackStatus == AckStatus.ACKED || curStatus.ackStatus == AckStatus.CANCELED) {
        return false;
      }

      updateOffsetStatus(curStatus, AckStatus.CANCELED);
      cancelableOffsets.remove(curStatus.offset);
      updateState();
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected boolean isFull(long offset) {
    return offsetStatusMap.size() >= capacity;
  }

  private long lowestCancelableOffset() {
    if (cancelableOffsets.isEmpty()) {
      return INITIAL_OFFSET;
    }

    return cancelableOffsets.iterator().next();
  }

  /**
   * Validates offset for Ack/Nack
   *
   * @return boolean indicate if offset is validated
   */
  private boolean validateOffset(long offset) {
    // condition 1: already committed before
    if (offset <= highestCommittedOffset) {
      return false;
    }

    // condition 2: the offset was not received before
    if (offset > highestReceivedOffset + 1) {
      return false;
    }

    return true;
  }

  private int size() {
    return offsetStatusMap.size();
  }

  private void updateOffsetStatus(OffsetStatus curStatus, AckStatus ackStatus) {
    if (curStatus.ackStatus == ackStatus) {
      return;
    }

    curStatus.setStatus(ackStatus);
  }

  private void updateState() {
    int size = size();
    if (size == 0) {
      state = new StateImpl(capacity, highestAckedOffset);
    } else {
      state =
          new StateImpl(
              capacity,
              size,
              headOffset,
              highestReceivedOffset,
              lowestCancelableOffset(),
              highestAckedOffset);
    }
  }

  /**
   * Stores in LinkedList to track received offsets and ack status To commit a offset, remove
   * corresponding OffsetStatus from list
   */
  private class OffsetStatus {
    /** received offsets */
    final long offset;

    /** ack status */
    AckStatus ackStatus = AckStatus.UNSET;

    private final Optional<String> key;

    /**
     * Instantiates an instance, with specific status
     *
     * @param offset the offset
     */
    private OffsetStatus(long offset, Optional<String> key) {
      this.offset = offset;
      this.key = key;
      onReceive(key);
    }

    private AckStatus setStatus(AckStatus ackStatus) {
      AckStatus curStatus = this.ackStatus;
      this.ackStatus = ackStatus;
      onStatusUpdate(curStatus, ackStatus, key);
      return curStatus;
    }

    private void close() {
      if (ackStatus == AckStatus.ACKED) {
        setStatus(AckStatus.UNSET);
      } else {
        // the only state a offset status can be cleared is ACKED
        throw new IllegalStateException("Clearing a non ACKED state");
      }
    }
  }
}
