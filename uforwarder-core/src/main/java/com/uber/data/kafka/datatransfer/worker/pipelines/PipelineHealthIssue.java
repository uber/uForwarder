package com.uber.data.kafka.datatransfer.worker.pipelines;

import java.util.HashSet;
import java.util.Set;

/**
 * All types consumer owned error that could cause delay of message delivery An issue comes with a
 * code, which is used to identify the issue type, start from 0
 */
public enum PipelineHealthIssue {
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

  private final int code;

  /** Pipeline health issue code */
  PipelineHealthIssue(int code) {
    this.code = code;
  }

  /**
   * Gets issue value the value can be used in bit set
   *
   * @return the issue name
   */
  public int value() {
    return 1 << code;
  }

  /**
   * Converts a bit sets into issues
   *
   * @param value
   * @return
   */
  public static Set<PipelineHealthIssue> decode(int value) {
    Set<PipelineHealthIssue> ret = new HashSet<>();
    for (PipelineHealthIssue issue : values()) {
      if ((value & issue.value()) != 0) {
        ret.add(issue);
      }
    }
    return ret;
  }
}
