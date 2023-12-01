package com.uber.data.kafka.instrumentation;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

// These tests are necessary for 100% test coverage, so we can enforce strict coverage rules
// for core libraries to improve code quality.
public class TagsTest extends FievelTestBase {
  @Test
  public void testKey() {
    Assert.assertEquals("reason", Tags.Key.reason);
    Assert.assertEquals("result", Tags.Key.result);
  }

  @Test
  public void testValue() {
    Assert.assertEquals("success", Tags.Value.success);
    Assert.assertEquals("failure", Tags.Value.failure);
  }

  @Test
  public void testClasses() {
    Tags tag = new Tags();
    Tags.Key key = new Tags.Key();
    Tags.Value value = new Tags.Value();
  }
}
