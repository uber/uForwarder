package com.uber.data.kafka.datatransfer.worker.pipelines;

/**
 * All types consumer owned error that could cause delay of message delivery An issue comes with a
 * code, which is used to identify the issue type, start from 0
 */
public enum KafkaPipelineIssue {
  /** Pipeline throttled by message rate limit */
  MESSAGE_RATE_LIMITED("message-rate-limited"),
  /** Pipeline rate throttled by bytes rate limit */
  BYTES_RATE_LIMITED("bytes-rate-limited"),
  /** Pipeline blocked by inflight message limit */
  INFLIGHT_MESSAGE_LIMITED("inflight-message-limited"),
  /** Authorization failed */
  PERMISSION_DENIED("permission-denied"),
  /** Unexpected error response received, indicates network error */
  INVALID_RESPONSE_RECEIVED("invalid-response-received"),
  /** Received retry response, but retry queue is not enabled */
  RETRY_WITHOUT_RETRY_QUEUE("retry-without-retry-queue"),
  /** Median RPC latency is above threshold determined by throughput and concurrency */
  MEDIAN_RPC_LATENCY_HIGH("median-rpc-latency-high"),
  /** Maximum RPC latency is above threshold determined by throughput and ack skew limit */
  MAX_RPC_LATENCY_HIGH("max-rpc-latency-high");

  private final PipelineHealthIssue healthIssue;

  /** Pipeline health issue code */
  KafkaPipelineIssue(String name) {
    this.healthIssue = new PipelineHealthIssue(name);
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
