package com.uber.data.kafka.datatransfer.common;

import com.google.protobuf.Message;
import java.util.Objects;
import org.apache.curator.x.async.modeled.versioned.Versioned;

/**
 * VersionedProto is an implementation of the Curator {@code Versioned} interface for proto data.
 *
 * @implNote For now, we use curator {@code Versioned} interface as our internal implementation for
 *     optimistic locking since ZK is our primary storage medium. In the future, we may abstract
 *     versioning away from the ZK implementation.
 */
public final class VersionedProto<M extends Message> implements Versioned<M> {
  private final M model;
  private final int version;

  private VersionedProto(M model, int version) {
    this.model = model;
    this.version = version;
  }

  @Override
  public M model() {
    return model;
  }

  @Override
  public int version() {
    return version;
  }

  public static <M extends Message> VersionedProto<M> from(final M model, final int version) {
    return new VersionedProto<M>(model, version);
  }

  public static <M extends Message> VersionedProto<M> from(final M model) {
    return new VersionedProto<M>(model, ZKUtils.NOOP_VERSION);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VersionedProto<?> that = (VersionedProto<?>) o;
    return version == that.version && Objects.equals(model, that.model);
  }

  @Override
  public int hashCode() {
    return Objects.hash(model, version);
  }
}
