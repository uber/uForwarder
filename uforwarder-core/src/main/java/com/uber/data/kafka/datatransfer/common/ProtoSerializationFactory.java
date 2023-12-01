package com.uber.data.kafka.datatransfer.common;

import com.google.api.core.InternalApi;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.io.UncheckedIOException;
import org.apache.curator.x.async.modeled.ModelSerializer;

/** Serialization factory for converting the protobuf messages to proto bytes and vice-versa. */
@InternalApi
public class ProtoSerializationFactory<M extends Message> implements ModelSerializer<M> {

  private final M prototype;

  /**
   * Creates {@link ProtoSerializationFactory} for a given proto type.
   *
   * @param prototype is the type of the protobuf object that should be serialized and deserialized
   */
  public ProtoSerializationFactory(M prototype) {
    this.prototype = prototype;
  }

  /**
   * Serialize the given proto model to proto bytes.
   *
   * @param model is the object to serialize
   * @return bytes[] of the serialized object
   */
  @Override
  public byte[] serialize(M model) {
    return model.toByteArray();
  }

  /**
   * Deserialize the proto bytes to a proto object.
   *
   * @param bytes to deserialize
   * @return the proto object
   * @throws UncheckedIOException on deserialization error
   */
  @Override
  @SuppressWarnings("unchecked")
  public M deserialize(byte[] bytes) {
    try {
      return (M) prototype.newBuilderForType().mergeFrom(bytes).build();
    } catch (InvalidProtocolBufferException e) {
      throw new UncheckedIOException(e);
    }
  }
}
