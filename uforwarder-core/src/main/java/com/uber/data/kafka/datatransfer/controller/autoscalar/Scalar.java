package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.api.core.InternalApi;
import com.google.protobuf.MessageOrBuilder;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import io.grpc.binarylog.v1.Message;

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

  /**
   * This method accepts a load measurement The load value represents the ratio between supply and
   * demand of capacity:
   *
   * <ul>
   *   <li>load &lt; 1.0 indicates demand is smaller than supply (underutilization)
   *   <li>load = 1.0 indicates demand matches supply (balanced)
   *   <li>load &gt; 1.0 indicates demand is more than supply (overutilization)
   * </ul>
   *
   * @param load
   */
  default void onLoad(double load) {}

  /**
   * Takes a dump of internal state of scalar for data analysis
   *
   * @return a snapshot of scalar
   */
  default MessageOrBuilder snapshot() {
    return Message.getDefaultInstance();
  }
}
