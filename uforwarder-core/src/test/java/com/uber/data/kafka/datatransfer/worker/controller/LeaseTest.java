package com.uber.data.kafka.datatransfer.worker.controller;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LeaseTest {
  private Lease lease;

  @BeforeEach
  public void setup() {
    lease = Lease.forTest(60000, 0);
  }

  @Test
  public void success() {
    // current time >> 0 so lease is not valid
    Assertions.assertFalse(lease.isValid());

    lease.success();

    // Marked lease as successful so it should now be valid
    Assertions.assertTrue(lease.isValid());
  }
}
