package com.uber.data.kafka.datatransfer.common;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

/**
 * Utility functions for operation on google protobuf timestamp.
 *
 * <p>This wraps google.protobuf.util.Timestamps.
 */
public final class TimestampUtils {
  /**
   * @return current timestamp in millisecond level granularity.
   */
  public static Timestamp currentTimeMilliseconds() {
    return Timestamps.fromMillis(System.currentTimeMillis());
  }

  /**
   * @return timestamp as a string in RFC 3339 format
   */
  public static String toString(Timestamp time) {
    return Timestamps.toString(time);
  }
}
