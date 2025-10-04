package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.api.core.InternalApi;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;

/**
 * Scalar applies scale to job Group. default scalar applies default scale to jobGroup autoscalar
 * computes and applies scale
 */
@InternalApi
public interface Scalar {
  double ZERO = 0.0d;
  Scalar DEFAULT = new Scalar() {};

  /**
   * computes then applies scale to a job group
   *
   * @param rebalancingJobGroup the job group
   * @param defaultScale the default scale if not able to compute scale
   */
  default void apply(RebalancingJobGroup rebalancingJobGroup, double defaultScale) {
    rebalancingJobGroup.updateScale(defaultScale, Throughput.ZERO);
  }
}
