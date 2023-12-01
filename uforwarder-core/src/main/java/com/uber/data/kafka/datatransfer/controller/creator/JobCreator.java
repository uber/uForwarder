package com.uber.data.kafka.datatransfer.controller.creator;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredJobGroup;
import com.uber.data.kafka.datatransfer.common.JobUtils;

/** JobCreator is an interface that can be implemented for pluggable job creation. */
public interface JobCreator {

  /**
   * Creates a StoredJob from the StoredJobGroup template, with jobId and jobKey set.
   *
   * @param storedJobGroup the template for the StoredJob
   * @param jobId the jobId for the StoredJob
   * @param jobKey the jobKey for the StoredJob
   * @return the newly created StoredJob
   * @implNote default implementation does not change other fields.
   */
  default StoredJob newJob(StoredJobGroup storedJobGroup, long jobId, int jobKey) {
    Job.Builder jobBuilder = JobUtils.newJobBuilder(storedJobGroup.getJobGroup());
    jobBuilder.setJobId(jobId);
    Job job = JobUtils.withJobKey(jobBuilder, jobKey);
    StoredJob.Builder builder = StoredJob.newBuilder();
    builder.setState(storedJobGroup.getState());
    builder.setJob(job);
    return builder.build();
  }
}
