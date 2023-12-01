package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.api.core.InternalApi;
import com.google.protobuf.Message;
import com.uber.data.kafka.datatransfer.common.JsonSerializationFactory;
import com.uber.data.kafka.datatransfer.common.ProtoSerializationFactory;
import org.apache.curator.x.async.modeled.ModelSerializer;

@InternalApi
public enum SerializerType {
  // Stores the objects as JSON in ZK
  JSON {
    @Override
    public <M extends Message> ModelSerializer<M> getSerializer(M prototype) {
      return new JsonSerializationFactory<>(prototype);
    }
  },
  // Stores the objects as Proto in ZK
  PROTO {
    @Override
    public <M extends Message> ModelSerializer<M> getSerializer(M prototype) {
      return new ProtoSerializationFactory<>(prototype);
    }
  };

  /**
   * Returns {@link ProtoSerializationFactory} if the type is {@link SerializerType#PROTO} and
   * returns {@link JsonSerializationFactory} if the type is {@link SerializerType#JSON}.
   *
   * @param prototype is the protobuf object where the type can be inferred
   * @param <M> is the type of the protobuf object
   * @return the serializer
   */
  public abstract <M extends Message> ModelSerializer<M> getSerializer(M prototype);
}
