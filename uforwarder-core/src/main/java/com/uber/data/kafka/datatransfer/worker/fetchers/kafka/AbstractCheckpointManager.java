package com.uber.data.kafka.datatransfer.worker.fetchers.kafka;

import com.uber.data.kafka.datatransfer.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** AbstractCheckpointManager implements get/update for checkpoint. */
abstract class AbstractCheckpointManager implements CheckpointManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCheckpointManager.class);

  @Override
  public void setFetchOffset(Job job, long fetchedOffset) {
    preConditionCheck(job);
    CheckpointInfo info = getCheckpointInfo(job);
    info.setFetchOffset(fetchedOffset);
  }

  @Override
  public void setOffsetToCommit(Job job, long commitOffset) {
    preConditionCheck(job);
    CheckpointInfo info = getCheckpointInfo(job);
    // only commit the highest one
    if (info.getOffsetToCommit() < commitOffset) {
      info.setOffsetToCommit(commitOffset);
    }
  }

  @Override
  public void setCommittedOffset(Job job, long committedOffset) {
    preConditionCheck(job);
    CheckpointInfo info = getCheckpointInfo(job);
    if (info.getCommittedOffset() < committedOffset) {
      info.setCommittedOffset(committedOffset);
    }
  }

  @Override
  public long getCommittedOffset(Job job) {
    preConditionCheck(job);
    CheckpointInfo info = getCheckpointInfo(job);
    return info.getCommittedOffset();
  }

  void preConditionCheck(Job job) {
    if (!job.hasKafkaConsumerTask()) {
      throw new IllegalArgumentException("KafkaConsumerTask required for CheckpointManager");
    }
  }
}
