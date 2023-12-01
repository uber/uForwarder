package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import java.util.Objects;

/**
 * RebalancingWorker represents a worker that has a reference to the total load currently assigned
 * to it.
 *
 * <p>This class implements Comparable that sorts from lowest to highest messages per sec used.
 * Note: this class has a natural ordering that is inconsistent with equals.
 */
class RebalancingWorker implements Comparable<RebalancingWorker> {

  private final long workerId;
  private final double load;

  RebalancingWorker(long workerId, double load) {
    this.workerId = workerId;
    this.load = load;
  }

  RebalancingWorker add(double load) {
    return new RebalancingWorker(getWorkerId(), getLoad() + load);
  }

  synchronized long getWorkerId() {
    return workerId;
  }

  synchronized double getLoad() {
    return load;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RebalancingWorker that = (RebalancingWorker) o;
    return workerId == that.workerId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(workerId);
  }

  @Override
  public int compareTo(RebalancingWorker o) {
    int result = Double.compare(this.getLoad(), o.getLoad());
    return result == 0 ? (int) (o.getWorkerId() - this.getWorkerId()) : result;
  }
}
