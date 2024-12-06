package com.uber.data.kafka.datatransfer.common;

import com.google.api.core.InternalApi;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.curator.x.async.modeled.ModelSerializer;

/**
 * JsonSerializationFactory abstract protobuf message serialization/deserialization to json.
 *
 * @implNote: We use JSON as the data format because it is easily consumed by human consumption. We
 *     use the protobuf JsonFormat, which handles schema evolution via optional JSON fields for us.
 */
@InternalApi
public final class JsonSerializationFactory<M extends Message> implements ModelSerializer<M> {
  private final M prototype;
  private final JsonFormat.TypeRegistry typeRegistry;

  /**
   * Create a new JsonSerializationFactory for object A.
   *
   * @param prototype of a protobuf object that should be serialized and deserialized. This object
   *     itself is a prototype and is not directly returned
   * @param typeRegistry the type registry
   */
  public JsonSerializationFactory(M prototype, JsonFormat.TypeRegistry typeRegistry) {
    this.prototype = prototype;
    this.typeRegistry = typeRegistry;
  }

  /**
   * Serialize Object object as json bytes.
   *
   * @param m is the object to serialize.
   * @return bytes[] of serialized object
   * @throws InvalidProtocolBufferException if there is an error in protobuf schema.
   */
  @Override
  public byte[] serialize(M m) {
    try {
      return JsonFormat.printer()
          .usingTypeRegistry(typeRegistry)
          .omittingInsignificantWhitespace()
          .print(m)
          .getBytes(StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Deserialize protobuf from json bytes.
   *
   * @param bytes to deserialize.
   * @return Object that was deserialized from the bytes.
   * @throws IOException on deserialization error.
   */
  @Override
  public M deserialize(byte[] bytes) {
    try {
      Message.Builder builder = prototype.newBuilderForType();
      JsonFormat.parser()
          .usingTypeRegistry(typeRegistry)
          .ignoringUnknownFields()
          .merge(
              new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8),
              builder);
      return (M) builder.build();
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}
