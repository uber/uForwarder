package com.uber.data.kafka.datatransfer.worker.pipelines;

/**
 * All types consumer owned error that could cause delay of message delivery An issue comes with a
 * code, which is used to identify the issue type, start from 0
 */
public enum KafkaPipelineIssue {
  /** Pipeline throttled by message rate limit */
  MESSAGE_RATE_LIMITED(0),
  /** Pipeline rate throttled by bytes rate limit */
  BYTES_RATE_LIMITED(1),
  /** Pipeline blocked by inflight message limit */
  INFLIGHT_MESSAGE_LIMITED(2),
  /** Authorization failed */
  PERMISSION_DENIED(3),
  /** Unexpected error response received, indicates network error */
  INVALID_RESPONSE_RECEIVED(4),
  /** Received retry response, but retry queue is not enabled */
  RETRY_WITHOUT_RETRY_QUEUE(5),
  /** RPC latency is unstable, p99 latency is much higher than p50 latency */
  RPC_LATENCY_UNSTABLE(6);

  private final PipelineHealthIssue healthIssue;

  /** Pipeline health issue code */
  KafkaPipelineIssue(int id) {
    this.healthIssue = new PipelineHealthIssue(id);
  }

  /**
   * Returns the corresponding PipelineHealthIssue
   *
   * @return
   */
  public PipelineHealthIssue getPipelineHealthIssue() {
    return healthIssue;
  }
}
