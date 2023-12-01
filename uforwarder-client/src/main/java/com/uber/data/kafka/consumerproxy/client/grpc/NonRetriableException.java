package com.uber.data.kafka.consumerproxy.client.grpc;

import io.grpc.Internal;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

@Internal
final class NonRetriableException {
  private NonRetriableException() {}

  static StatusRuntimeException of() {
    return Status.FAILED_PRECONDITION.asRuntimeException();
  }
}
