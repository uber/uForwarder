package com.uber.data.kafka.datatransfer.controller.rpc;

import com.uber.data.kafka.datatransfer.Job;

/** {@link JobThroughputSink} consumes throughout of jobs reported by worker */
public interface JobThroughputSink {
  // no operation sink
  JobThroughputSink NOOP = (job, messagesPerSecond, bytesPerSecond) -> {};

  /**
   * Consumes throughout of jobs reported by worker
   *
   * @param job the job
   * @param messagesPerSecond the messages per second
   * @param bytesPerSecond the bytes per second
   */
  void consume(Job job, double messagesPerSecond, double bytesPerSecond);
}
