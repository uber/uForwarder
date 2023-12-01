package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import java.util.Objects;

public class DispatcherResponseAndOffset extends DispatcherResponse {
  private final long offset;

  public DispatcherResponseAndOffset(Code code, long offset) {
    super(code);
    this.offset = offset;
  }

  public DispatcherResponseAndOffset(Code code) {
    this(code, -1);
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DispatcherResponseAndOffset that = (DispatcherResponseAndOffset) o;
    return offset == that.offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), offset);
  }
}
