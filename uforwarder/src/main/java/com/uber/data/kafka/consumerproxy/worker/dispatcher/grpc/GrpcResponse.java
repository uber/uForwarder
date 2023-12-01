package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.Optional;

/** GrpcResponse wraps {@link GrpcDispatcher} response from consumer-proxy-client. */
public class GrpcResponse {
  private final Status status;
  private final Optional<Metadata> trailers;
  private final Optional<DispatcherResponse.Code> code;
  private final boolean overDue;

  private GrpcResponse(
      Status status,
      Optional<Metadata> trailers,
      boolean overDue,
      Optional<DispatcherResponse.Code> code) {
    this.status = status;
    this.trailers = trailers;
    this.overDue = overDue;
    this.code = code;
  }

  /**
   * Constructs GrpcResponse with OK status
   *
   * @return
   */
  public static GrpcResponse of() {
    return new GrpcResponse(Status.OK, Optional.empty(), false, Optional.empty());
  }

  /**
   * Constructs GrpcResponse with response status only
   *
   * @param status the GRPC status
   * @return the grpc response
   */
  public static GrpcResponse of(Status status) {
    return new GrpcResponse(status, Optional.empty(), false, Optional.empty());
  }

  /**
   * Constructs GrpcResponse with response status, trailers and overDue
   *
   * @param status the GRPC status
   * @param overDue the response exceeds deadline
   * @return the grpc response
   */
  public static GrpcResponse of(Status status, Optional<Metadata> trailers, boolean overDue) {
    return new GrpcResponse(status, trailers, overDue, Optional.empty());
  }

  /**
   * Constructs GrpcResponse with both response status and dispatcher code
   *
   * @param status the GRPC status
   * @param code the dispatcher code
   * @return the grpc response
   */
  public static GrpcResponse of(Status status, DispatcherResponse.Code code) {
    return new GrpcResponse(status, Optional.empty(), false, Optional.of(code));
  }

  /**
   * Gets GRPC status.
   *
   * @return the status
   */
  public Status status() {
    return status;
  }

  /**
   * Gets optional {@link DispatcherResponse} code
   *
   * @return the optional
   */
  public Optional<DispatcherResponse.Code> code() {
    return code;
  }

  /**
   * Is the Grpc request actually timeout
   *
   * @return the boolean
   */
  public boolean isOverDue() {
    return overDue;
  }

  /**
   * Gets optional metadata
   *
   * @return
   */
  public Optional<Metadata> trailers() {
    return trailers;
  }
}
