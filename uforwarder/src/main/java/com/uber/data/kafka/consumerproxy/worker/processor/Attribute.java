package com.uber.data.kafka.consumerproxy.worker.processor;

/** A wrapper class for attribute value. */
public class Attribute<T> {

  private final T value;

  public Attribute(final T value) {
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (o instanceof Attribute) {
      return value.equals(((Attribute) o).value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
