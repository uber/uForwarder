package com.uber.data.kafka.instrumentation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

// These tests are necessary for 100% test coverage, so we can enforce strict coverage rules
// for core libraries to improve code quality.
public class TagsTest {
  @Test
  public void testKey() {
    Assertions.assertEquals("reason", Tags.Key.reason);
    Assertions.assertEquals("result", Tags.Key.result);
  }

  @Test
  public void testValue() {
    Assertions.assertEquals("success", Tags.Value.success);
    Assertions.assertEquals("failure", Tags.Value.failure);
  }

  @Test
  public void testClasses() {
    Tags tag = new Tags();
    Tags.Key key = new Tags.Key();
    Tags.Value value = new Tags.Value();
  }
}
