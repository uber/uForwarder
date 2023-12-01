package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import java.util.Objects;

/*
 * RebalancingJob represents a job that is being rebalanced.
 *
 * <p>This class implements Comparable that sorts from highest to lowest messages per sec
 * requirement. Note: this class has a natural ordering that is inconsistent with equals.
 */
class RebalancingJob implements Comparable<RebalancingJob> {

  private volatile StoredJob job;
  private final RebalancingJobGroup jobGroup;
  private double load;

  RebalancingJob(StoredJob job, RebalancingJobGroup jobGroup) {
    this.job = job;
    this.jobGroup = jobGroup;
    this.load = job.getScale();
  }

  synchronized double getLoad() {
    return load;
  }

  synchronized long getJobId() {
    return job.getJob().getJobId();
  }

  synchronized long getWorkerId() {
    return job.getWorkerId();
  }

  synchronized String getRpcUri() {
    return job.getJob().getRpcDispatcherTask().getUri();
  }

  synchronized JobState getJobState() {
    return job.getState();
  }

  RebalancingJobGroup getJobGroup() {
    return jobGroup;
  }

  synchronized void setWorkerId(long workerId) {
    job = job.toBuilder().setWorkerId(workerId).build();
    jobGroup.updateJob(job.getJob().getJobId(), job);
  }

  @Override
  public int hashCode() {
    return Objects.hash(job.getJob().getJobId());
  }

  @Override
  public int compareTo(RebalancingJob o) {
    if (Double.valueOf(o.getLoad()).equals(this.getLoad())) {
      // if the load is equal, we can compare the job id to determine an order
      return Long.compare(o.getJobId(), this.getJobId());
    }
    return Double.compare(o.getLoad(), this.getLoad());
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RebalancingJob that = (RebalancingJob) o;
    return job.getJob().getJobId() == that.job.getJob().getJobId();
  }
}
