package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DispatcherResponseTest extends FievelTestBase {
  private DispatcherResponse responseCommit;
  private DispatcherResponse responseDLQ;

  @Before
  public void setup() {
    responseCommit = new DispatcherResponse(DispatcherResponse.Code.COMMIT);
    responseDLQ = new DispatcherResponse(DispatcherResponse.Code.DLQ);
  }

  @Test
  public void testGetCode() {
    Assert.assertEquals(DispatcherResponse.Code.COMMIT, responseCommit.getCode());
  }

  @Test
  public void testEquals() {
    Assert.assertEquals(responseCommit, responseCommit);
    Assert.assertNotEquals(responseCommit, responseDLQ);
    Assert.assertNotEquals(responseCommit, null);
    Assert.assertNotEquals(null, responseCommit);
    Assert.assertNotEquals(responseCommit, new Object());
    Assert.assertNotEquals(new Object(), responseCommit);
  }

  @Test
  public void testHashCode() {
    Assert.assertEquals(responseCommit.hashCode(), responseCommit.hashCode());
    Assert.assertNotEquals(responseCommit.hashCode(), responseDLQ.hashCode());
  }

  @Test
  public void testCodeValue() {
    Assert.assertEquals(0, DispatcherResponse.Code.DLQ.ordinal());
    Assert.assertEquals(1, DispatcherResponse.Code.RETRY.ordinal());
    Assert.assertEquals(2, DispatcherResponse.Code.RESQ.ordinal());
    Assert.assertEquals(3, DispatcherResponse.Code.BACKOFF.ordinal());
    Assert.assertEquals(4, DispatcherResponse.Code.OVERLOADED.ordinal());
    Assert.assertEquals(5, DispatcherResponse.Code.INVALID.ordinal());
    Assert.assertEquals(6, DispatcherResponse.Code.SKIP.ordinal());
    Assert.assertEquals(7, DispatcherResponse.Code.COMMIT.ordinal());
  }
}
