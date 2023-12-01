package com.uber.data.kafka.consumerproxy.controller.rebalancer;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.consumerproxy.config.RebalancerConfiguration;
import com.uber.data.kafka.datatransfer.JobState;
import com.uber.data.kafka.datatransfer.StoredJob;
import com.uber.data.kafka.datatransfer.StoredWorker;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.data.kafka.datatransfer.controller.rebalancer.RebalancingJobGroup;
import com.uber.m3.tally.Scope;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StreamingRpcUriRebalancer extends {@link AbstractRpcUriRebalancer}.
 *
 * <p>It updates job state to the job group state if
 *
 * <ol>
 *   <li>the job is not in canceled state
 *   <li>the job is in a different state
 * </ol>
 */
public class StreamingRpcUriRebalancer extends AbstractRpcUriRebalancer {
  private static final Logger logger = LoggerFactory.getLogger(StreamingRpcUriRebalancer.class);
  private final Scope scope;

  public StreamingRpcUriRebalancer(
      Scope scope,
      RebalancerConfiguration config,
      Scalar scalar,
      HibernatingJobRebalancer hibernatingJobRebalancer) {
    super(scope, config, scalar, hibernatingJobRebalancer);
    this.scope = scope.tagged(ImmutableMap.of("rebalancerType", "StreamingRpcUriRebalancer"));
  }

  @Override
  public void computeJobState(
      final Map<String, RebalancingJobGroup> jobGroups, final Map<Long, StoredWorker> workers)
      throws Exception {
    for (RebalancingJobGroup rebalancingJobGroup : jobGroups.values()) {
      JobState jobGroupState = rebalancingJobGroup.getJobGroupState();
      for (Map.Entry<Long, StoredJob> jobEntry : rebalancingJobGroup.getJobs().entrySet()) {
        long jobId = jobEntry.getKey();
        StoredJob job = jobEntry.getValue();
        // update the job state to match the job group state when:
        // 1. job group state != job state
        // 2. job state is not canceled (canceled is a terminal state).
        if (job.getState() != JobState.JOB_STATE_CANCELED && job.getState() != jobGroupState) {
          rebalancingJobGroup.updateJob(jobId, job.toBuilder().setState(jobGroupState).build());
        }
      }
    }
  }
}
