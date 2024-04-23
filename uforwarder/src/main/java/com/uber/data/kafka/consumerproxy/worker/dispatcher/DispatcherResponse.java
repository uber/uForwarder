package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.uber.m3.tally.Buckets;
import com.uber.m3.tally.ValueBuckets;
import java.util.Arrays;
import java.util.Objects;

/** DispatcherResponse wraps the response received from {@code Dispatcher}. */
public class DispatcherResponse {
  public static final Buckets<Double> responseCodeDistributionBuckets =
      new ValueBuckets(
          Arrays.stream(Code.values()).map(r -> r.ordinal() + 1.0).toArray(Double[]::new));

  private final Code code;

  public DispatcherResponse(Code code) {
    this.code = code;
  }

  public Code getCode() {
    return code;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DispatcherResponse that = (DispatcherResponse) o;
    return code == that.code;
  }

  @Override
  public int hashCode() {
    return Objects.hash(code);
  }

  public enum Code {
    DLQ,
    RETRY,
    RESQ,
    BACKOFF,
    OVERLOADED, // handler is shedding load
    INVALID,
    SKIP,
    COMMIT
  }
}
