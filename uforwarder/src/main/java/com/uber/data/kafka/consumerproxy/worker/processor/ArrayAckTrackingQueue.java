package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.clients.admin.VisibleForTesting;
import com.uber.data.kafka.consumerproxy.common.StructuredLogging;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.m3.tally.Scope;
import java.util.Arrays;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ArrayAckTrackingQueue implements {@link AckTrackingQueue} in array
 *
 * <p>ArrayAckTrackingQueue assume messages received is increasing consecutively If offset
 * decreases, it will be omitted If offset has gap between previous offset, this implementation
 * assumes that some offsets were purged from Kafka servers, and resets the ack tracking queue.
 */
final class ArrayAckTrackingQueue extends AbstractAckTrackingQueue {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrayAckTrackingQueue.class);
  /** the queue to store all elements */
  final OffsetStatus[] items;

  /** items index for head element */
  volatile int headIndex;

  /** Offset corresponding to the head index */
  volatile long offsetMappingHeadIndex;

  /** lowest message offset can be cancelled */
  volatile long lowestCancelableOffset;

  /** highest message offset received from kafka server */
  volatile long highestReceivedOffset;

  /** highest message offset acked */
  volatile long highestAckedOffset;

  ArrayAckTrackingQueue(Job job, int capacity, Scope rootScope) {
    super(job, capacity, rootScope, LOGGER);
    // initialize the ack status queue
    this.items = new OffsetStatus[capacity];
    reset();

    // initialize those changeable variables
    this.offsetMappingHeadIndex = INITIAL_OFFSET;
    this.lowestCancelableOffset = INITIAL_OFFSET;
    this.highestReceivedOffset = INITIAL_OFFSET;
    this.highestAckedOffset = INITIAL_OFFSET;
  }

  /**
   * If offsets are incrementally received, this method updates the highest message offset received.
   * If not, this method assumes that some offsets were purged from Kafka servers, and resets the
   * ack tracking queue.
   *
   * <ol>
   *   <li>this method blocks when the offset to receive goes to far ( >= the first unacked offset +
   *       capacity)
   *   <li>an {@code AckTrackingQueue#ack} unblocks the blocked {@code AckTrackingQueue#receive}
   *       when it makes (the offset to receive < the first unacked offset + capacity)
   *   <li>an {@code AckTrackingQueue#markAsNotInUse} unblocks the blocked {@code
   *       AckTrackingQueue#receive}
   * </ol>
   *
   * @param offset the offset of a newly-received message
   */
  @Override
  public synchronized void receive(long offset, Optional<String> key) throws InterruptedException {
    lock.lock();
    try {
      if (highestReceivedOffset != INITIAL_OFFSET) {
        if (offset - highestReceivedOffset != 1) {
          scope
              .gauge(AckTrackingQueue.MetricNames.OFFSET_RECEIVED_GAP)
              .update(offset - highestReceivedOffset);
          LOGGER.warn(
              "gaps between received offsets",
              StructuredLogging.offsetGap(offset - highestReceivedOffset));
        }
      }
      if (highestReceivedOffset < offset) {
        scope.gauge(AckTrackingQueue.MetricNames.HIGHEST_OFFSET_RECEIVED).update(offset);
        // the first time to receive
        if (highestReceivedOffset == INITIAL_OFFSET) {
          this.offsetMappingHeadIndex = offset;
          this.lowestCancelableOffset = offset;
          this.highestReceivedOffset = offset;
          this.highestAckedOffset = offset;
        } else if (offset - highestReceivedOffset != 1) {
          // offsets should be received in order.
          // If a new offset is too large, it means that some offsets are purged from Kafka servers,
          // so we need to reset the ack tracking queue.
          this.offsetMappingHeadIndex = offset;
          this.lowestCancelableOffset = offset;
          this.highestReceivedOffset = offset;
          this.highestAckedOffset = offset;
          reset();
          notFull.signalAll();
        }
        // if we cannot mark more offsets as received
        // action:  wait until this offset can be received (then proceed) or this queue is not in
        // use
        // anymore (then return)
        if (waitForNotFull(offset + 1)) {
          // update offset key;
          if (key.isPresent()) {
            OffsetStatus status =
                items[(int) (offset - offsetMappingHeadIndex + headIndex) % capacity];
            status.key = key;
          }
          onReceive(key);
          highestReceivedOffset = offset;
        }
        updateState();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long ack(long offset) throws InterruptedException {
    lock.lock();
    try {
      if (!validateOffset(offset)) {
        return CANNOT_ACK;
      }

      scope.gauge(MetricNames.IN_MEMORY_CAPACITY).update(capacity);
      // condition 5: this offset is the head of the ack queue
      // action:
      // (1) move head to the next un-acked offset
      // (2) adjust offsetMappingHeadIndex
      long ret;
      if (offset == offsetMappingHeadIndex + 1) {
        setAckStatus(headIndex, AckStatus.ACKED);
        while (items[headIndex].ackStatus == AckStatus.ACKED) {
          setAckStatus(headIndex, AckStatus.UNSET);
          headIndex = increaseIndex(headIndex);
          offsetMappingHeadIndex++;
        }
        if (lowestCancelableOffset < offsetMappingHeadIndex) {
          lowestCancelableOffset = offsetMappingHeadIndex;
        }
        updateLowestCancelableOffset(offsetMappingHeadIndex + 1);
        notFull.signalAll();
        scope
            .gauge(AckTrackingQueue.MetricNames.HIGHEST_OFFSET_COMMITTED)
            .update(offsetMappingHeadIndex);
        scope.gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED).update(size());
        ret = offsetMappingHeadIndex;
      } else {
        // condition 6: this offset is in the middle of the ack queue
        // action: just ack it
        int index = (int) (offset - 1 - offsetMappingHeadIndex + headIndex) % capacity;
        if (items[index].ackStatus == AckStatus.ACKED) {
          ret = DUPLICATED_ACK;
        } else {
          setAckStatus(index, AckStatus.ACKED);
          updateLowestCancelableOffset(offset);
          if (highestAckedOffset < offset) {
            highestAckedOffset = offset;
            scope.gauge(AckTrackingQueue.MetricNames.HIGHEST_OFFSET_ACKED).update(offset);
            scope.gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED).update(size());
          }
          ret = IN_MEMORY_ACK_ONLY;
        }
      }
      updateState();
      return ret;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean nack(long offset) throws InterruptedException {
    lock.lock();
    try {
      if (!validateOffset(offset)) {
        return false;
      }

      // condition 5: this offset is in the middle of the ack queue
      int index = (int) (offset - 1 - offsetMappingHeadIndex + headIndex) % capacity;
      // condition 5.1: this offset has been nacked or acked before
      if (items[index].ackStatus != AckStatus.UNSET) {
        return false;
      }
      // condition 5.2: this offset can be nacked now.
      setAckStatus(index, AckStatus.NACKED);
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean cancel(long offset) throws InterruptedException {
    lock.lock();
    try {
      if (!validateOffset(offset)) {
        return false;
      }
      int index = (int) (offset - 1 - offsetMappingHeadIndex + headIndex) % capacity;
      if (items[index].ackStatus == AckStatus.ACKED
          || items[index].ackStatus == AckStatus.CANCELED) {
        // offset is already acked or canceled
        return false;
      }
      setAckStatus(index, AckStatus.CANCELED);
      // move lowest cancel offset forward to first offset that is not acked nor canceled
      updateLowestCancelableOffset(offset);
      updateState();
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected boolean isFull(long offset) {
    return offset - offsetMappingHeadIndex - 1 >= capacity;
  }

  /**
   * Move lowest cancelable offset
   *
   * @param offset acked or canceled offset
   */
  private void updateLowestCancelableOffset(long offset) {
    if (offset == lowestCancelableOffset + 1) {
      int nextCancelIndex =
          (int) (lowestCancelableOffset - offsetMappingHeadIndex + headIndex) % capacity;
      while (items[nextCancelIndex].ackStatus == AckStatus.ACKED
          || items[nextCancelIndex].ackStatus == AckStatus.CANCELED) {
        nextCancelIndex = increaseIndex(nextCancelIndex);
        lowestCancelableOffset++;
        if (lowestCancelableOffset > highestReceivedOffset) {
          break;
        }
      }
    }
  }

  private long lowestCancelableOffset() {
    if (lowestCancelableOffset > highestReceivedOffset) {
      return INITIAL_OFFSET;
    }

    return lowestCancelableOffset;
  }

  protected void updateState() {
    int size = size();
    if (size == 0) {
      state = new StateImpl(capacity, highestAckedOffset);
    } else {
      state =
          new StateImpl(
              capacity,
              size,
              offsetMappingHeadIndex,
              highestReceivedOffset,
              lowestCancelableOffset(),
              highestAckedOffset);
    }
  }

  /**
   * Validates offset for Ack/Nack
   *
   * @return boolean indicate if offset is validated
   */
  private boolean validateOffset(long offset) throws InterruptedException {
    // condition 1: have never received messages before
    if (highestReceivedOffset == INITIAL_OFFSET) {
      return false;
    }

    // condition 2: the offset was not received before
    if (offset > highestReceivedOffset + 1) {
      return false;
    }

    // condition 3: the offset was received before, but it's too large now (this should not
    // happen, but we take it into consideration for correctness)
    // action:  wait until this offset can be acked (then proceed) or this queue is not in use
    // anymore (then return)
    if (!waitForNotFull(offset)) {
      return false;
    }

    // condition 4: the offset has been acked before
    if (offset < offsetMappingHeadIndex + 1) {
      return false;
    }

    return true;
  }

  private int size() {
    return (int) Math.max(0L, highestReceivedOffset - offsetMappingHeadIndex + 1);
  }

  private int width() {
    if (highestAckedOffset == INITIAL_OFFSET) {
      return 0;
    }
    return (int) Math.max(0L, highestAckedOffset - offsetMappingHeadIndex - 1);
  }

  private int increaseIndex(int previousIndex) {
    return (++previousIndex) % capacity;
  }

  private void reset() {
    this.headIndex = 0;
    resetRuntimeStats();
    Arrays.setAll(this.items, i -> new OffsetStatus());
    reactors.stream().forEach(reactor -> reactor.onReset());
  }

  private void setAckStatus(int index, AckStatus ackStatus) {
    OffsetStatus curStatus = items[index];
    if (curStatus.ackStatus == ackStatus) {
      return;
    }

    curStatus.setStatus(ackStatus);
  }

  /** */
  @VisibleForTesting
  protected class OffsetStatus {
    /** ack status */
    AckStatus ackStatus = AckStatus.UNSET;

    private Optional<String> key = Optional.empty();

    protected AckStatus setStatus(AckStatus ackStatus) {
      AckStatus curStatus = this.ackStatus;
      onStatusUpdate(curStatus, ackStatus, key);
      if (ackStatus == AckStatus.UNSET) {
        // reset key
        key = Optional.empty();
      }
      this.ackStatus = ackStatus;
      return curStatus;
    }
  }
}
