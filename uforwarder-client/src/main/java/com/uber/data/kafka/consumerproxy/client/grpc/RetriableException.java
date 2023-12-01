package com.uber.data.kafka.consumerproxy.client.grpc;

import io.grpc.Internal;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

@Internal
final class RetriableException {
  private RetriableException() {}

  static StatusRuntimeException of() {
    return Status.RESOURCE_EXHAUSTED.asRuntimeException();
  }
}
