package com.uber.data.kafka.datatransfer.controller.creator;

import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.KafkaUtils;
import com.uber.m3.tally.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingJobCreator creates a StoredJob using the given StoredJobGroup as a template,
 *
 * <ol>
 *   <li>sets jobId and partition to the given values.
 *   <li>sets start offset to -1 and end offset to 0 for the kafka consumer.
 * </ol>
 */
public class StreamingJobCreator extends JobCreatorWithOffsets {
  private static final Logger logger = LoggerFactory.getLogger(StreamingJobCreator.class);
  private static final String JOB_TYPE = "streaming";
  private final Scope scope;

  public StreamingJobCreator(Scope scope) {
    this.scope = scope;
  }

  @Override
  public StoredJob newJob(StoredJobGroup storedJobGroup, long jobId, int partition) {
    return newJob(
        scope,
        logger,
        storedJobGroup,
        JOB_TYPE,
        jobId,
        partition,
        KafkaUtils.MAX_INVALID_START_OFFSET,
        KafkaUtils.MAX_INVALID_END_OFFSET);
  }
}
