package com.uber.data.kafka.consumerproxy.worker.processor;

/** A wrapper class for attribute key. */
public class AttributeKey {
  private final String key;

  /**
   * Constructor.
   *
   * @param key attribute key
   */
  public AttributeKey(final String key) {
    this.key = key;
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof AttributeKey) {
      return key.equals(((AttributeKey) o).key);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public String toString() {
    return key;
  }
}
