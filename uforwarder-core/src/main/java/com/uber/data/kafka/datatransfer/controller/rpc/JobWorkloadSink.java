package com.uber.data.kafka.datatransfer.controller.rpc;

import com.uber.data.kafka.datatransfer.Job;

/** {@link JobWorkloadSink} consumes activity metrics of jobs reported by worker */
public interface JobWorkloadSink {
  // no operation sink
  JobWorkloadSink NOOP = (job, activity) -> {};

  /**
   * Consumes activity metrics of jobs reported by worker
   *
   * @param job the job
   * @param workload the job workload
   */
  void consume(Job job, Workload workload);
}
