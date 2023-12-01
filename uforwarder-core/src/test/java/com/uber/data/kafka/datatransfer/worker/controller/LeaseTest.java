package com.uber.data.kafka.datatransfer.worker.controller;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LeaseTest extends FievelTestBase {
  private Lease lease;

  @Before
  public void setup() {
    lease = Lease.forTest(60000, 0);
  }

  @Test
  public void success() {
    // current time >> 0 so lease is not valid
    Assert.assertFalse(lease.isValid());

    lease.success();

    // Marked lease as successful so it should now be valid
    Assert.assertTrue(lease.isValid());
  }
}
