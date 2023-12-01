package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * RebalancingWorkerWithSortedJobs represents a worker that has a reference to the total load
 * currently assigned to it and the sorted list of all the jobs in descending order.
 *
 * <p>This class implements Comparable that sorts from lowest to highest messages per sec used.
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
class RebalancingWorkerWithSortedJobs implements Comparable<RebalancingWorkerWithSortedJobs> {

  private final long workerId;
  private double load;
  private final TreeSet<RebalancingJob> jobs;

  RebalancingWorkerWithSortedJobs(long workerId, double load, List<RebalancingJob> jobs) {
    this.workerId = workerId;
    this.load = load;
    this.jobs = new TreeSet<>();
    this.jobs.addAll(jobs);
  }

  void addJob(RebalancingJob job) {
    this.jobs.add(job);
    this.load += job.getLoad();
  }

  List<RebalancingJob> getAllJobs() {
    return new ArrayList<>(jobs);
  }

  int getNumberOfJobs() {
    return jobs.size();
  }

  void removeJob(RebalancingJob rebalancingJob) {
    jobs.remove(rebalancingJob);
    load -= rebalancingJob.getLoad();
  }

  long getWorkerId() {
    return workerId;
  }

  double getLoad() {
    return load;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RebalancingWorkerWithSortedJobs that = (RebalancingWorkerWithSortedJobs) o;
    return workerId == that.workerId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(workerId);
  }

  @Override
  public int compareTo(RebalancingWorkerWithSortedJobs o) {
    int result = Double.compare(this.getLoad(), o.getLoad());
    if (result != 0) {
      return result;
    }

    result = Integer.compare(this.getNumberOfJobs(), o.getNumberOfJobs());
    return result == 0 ? (int) (this.getWorkerId() - o.getWorkerId()) : result;
  }
}
