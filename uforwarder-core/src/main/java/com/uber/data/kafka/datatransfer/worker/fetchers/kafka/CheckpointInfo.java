package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

/** Class to store in-memory topic partition offset info for kafka */
public final class CheckpointInfo {

  private final Job job;

  // message fetch offset
  private final AtomicLong fetchOffset;
  // message offset that is ready to commit
  private final AtomicLong offsetToCommit;

  // message offset that is already persisted into offset store.
  private final AtomicLong committedOffset;

  // starting offset for consumer fetcher thread to startwith, -1 means honor commit offset at kafka
  // consumer.
  private long startingOffset;

  // end offset for task
  @Nullable private final Long endOffset;

  CheckpointInfo(Job job, long startingOffset, @Nullable Long endOffset) {
    this.job = job;
    this.startingOffset = startingOffset;
    this.endOffset = endOffset;
    this.fetchOffset = new AtomicLong(startingOffset);
    this.offsetToCommit = new AtomicLong(startingOffset);
    this.committedOffset = new AtomicLong(startingOffset);
  }

  public void setFetchOffset(Long fetchedOffset) {
    this.fetchOffset.set(fetchedOffset);
  }

  public void setOffsetToCommit(Long offsetToCommit) {
    this.offsetToCommit.set(offsetToCommit);
  }

  public void setCommittedOffset(Long commitedOffset) {
    this.committedOffset.set(commitedOffset);
  }

  public boolean bounded(Long currentOffset) {
    return this.endOffset != null && this.endOffset <= currentOffset;
  }

  public long getStartingOffset() {
    return startingOffset;
  }

  public long getFetchOffset() {
    return fetchOffset.get();
  }

  public long getOffsetToCommit() {
    return offsetToCommit.get();
  }

  public long getCommittedOffset() {
    return committedOffset.get();
  }

  /**
   * Checks if the offset exists in the checkpoint info
   *
   * @return true if the offset exists in the checkpoint info
   */
  public boolean isCommitOffsetExists() {
    return offsetToCommit.get() != KafkaUtils.MAX_INVALID_START_OFFSET
        || committedOffset.get() != KafkaUtils.MAX_INVALID_START_OFFSET;
  }

  public Job getJob() {
    return job;
  }
}
