package com.uber.data.kafka.datatransfer.utils;

import org.junit.runners.model.InitializationError;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/** TODO: remove once moving out of fievel repo */
public class UForwarderSpringJUnit4ClassRunner extends SpringJUnit4ClassRunner {

  /**
   * Construct a new {@code SpringJUnit4ClassRunner} and initialize a {@link TestContextManager} to
   * provide Spring testing functionality to standard JUnit tests.
   *
   * @param clazz the test class to be run
   * @see #createTestContextManager(Class)
   */
  public UForwarderSpringJUnit4ClassRunner(Class<?> clazz) throws InitializationError {
    super(clazz);
  }
}
