package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.api.core.InternalApi;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.common.JsonSerializationFactory;
import com.uber.data.kafka.datatransfer.common.ProtoSerializationFactory;
import org.apache.curator.x.async.modeled.ModelSerializer;

@InternalApi
public enum SerializerType {
  // Stores the objects as JSON in ZK
  JSON {
    @Override
    public <M extends Message> ModelSerializer<M> getSerializer(
        M prototype, JsonFormat.TypeRegistry typeRegistry) {
      return new JsonSerializationFactory<>(prototype, typeRegistry);
    }
  },
  // Stores the objects as Proto in ZK
  PROTO {
    @Override
    public <M extends Message> ModelSerializer<M> getSerializer(
        M prototype, JsonFormat.TypeRegistry typeRegistry) {
      return new ProtoSerializationFactory<>(prototype);
    }
  };

  /**
   * Returns {@link ProtoSerializationFactory} if the type is {@link SerializerType#PROTO} and
   * returns {@link JsonSerializationFactory} if the type is {@link SerializerType#JSON}.
   *
   * @param <M> is the type of the protobuf object
   * @param prototype is the protobuf object where the type can be inferred
   * @param typeRegistry the type registry
   * @return the serializer
   */
  public abstract <M extends Message> ModelSerializer<M> getSerializer(
      M prototype, JsonFormat.TypeRegistry typeRegistry);

  /**
   * Returns {@link ProtoSerializationFactory} if the type is {@link SerializerType#PROTO} and
   * returns {@link JsonSerializationFactory} if the type is {@link SerializerType#JSON}.
   *
   * @param <M> is the type of the protobuf object
   * @param prototype is the protobuf object where the type can be inferred
   * @return the serializer
   */
  public <M extends Message> ModelSerializer<M> getSerializer(M prototype) {
    return getSerializer(prototype, JsonFormat.TypeRegistry.getEmptyTypeRegistry());
  }
}
