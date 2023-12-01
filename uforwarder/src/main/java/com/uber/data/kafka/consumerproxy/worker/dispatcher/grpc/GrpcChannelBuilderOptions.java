package com.uber.data.kafka.consumerproxy.worker.dispatcher.grpc;

/** Options for the GrpcChannelBuilder */
public abstract class GrpcChannelBuilderOptions {
  // empty params
  public static GrpcChannelBuilderOptions DEFAULT = new GrpcChannelBuilderOptions() {};

  protected GrpcChannelBuilderOptions() {}
}
