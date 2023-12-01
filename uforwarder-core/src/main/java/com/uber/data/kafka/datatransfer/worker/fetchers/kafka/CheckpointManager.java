package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.Job;
import java.io.Closeable;
import javax.validation.constraints.NotNull;

/** CheckpointManager keeps track of checkpoints. */
public interface CheckpointManager extends Closeable {
  /**
   * Updates massage offset on job to mark messages before this offset has been successfully
   * processed and enqueue to next stage by fetcher
   */
  void setFetchOffset(Job job, long fetchedOffset);

  /** Updates offset to be committed. */
  void setOffsetToCommit(Job job, long commitOffset);

  /** gets the offset to be committed. */
  long getOffsetToCommit(Job job);

  /** gets the offset that has been committed. */
  long getCommittedOffset(Job job);

  /** Updates offset that has been committed. */
  void setCommittedOffset(Job job, long committedOffset);

  /** Gets checkpoint info */
  @NotNull
  CheckpointInfo getCheckpointInfo(Job job);

  /** Adds checkpoint info */
  @NotNull
  CheckpointInfo addCheckpointInfo(Job job);
}
