package com.uber.data.kafka.datatransfer.common;

import io.grpc.ClientInterceptor;
import io.grpc.Metadata;

public class MetadataUtils {
  public static ClientInterceptor metadataInterceptor(String key, byte[] value) {
    return io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(metadata(key, value));
  }

  public static ClientInterceptor metadataInterceptor(String key, String value) {
    return io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(metadata(key, value));
  }

  public static Metadata metadata(String key, byte[] value) {
    Metadata metadata = new Metadata();
    Metadata.Key<byte[]> metadataKey = Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER);
    metadata.put(metadataKey, value);
    return metadata;
  }

  public static Metadata metadata(String key, String value) {
    Metadata metadata = new Metadata();
    Metadata.Key<String> metadataKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
    metadata.put(metadataKey, value);
    return metadata;
  }
}
