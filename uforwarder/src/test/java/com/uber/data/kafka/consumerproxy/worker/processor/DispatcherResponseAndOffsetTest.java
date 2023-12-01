package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DispatcherResponseAndOffsetTest extends FievelTestBase {
  DispatcherResponseAndOffset dispatcherResponseAndOffset;
  DispatcherResponseAndOffset sameDispatcherResponseAndOffset;
  DispatcherResponseAndOffset differentDispatcherResponseAndOffset;

  @Before
  public void setup() {
    dispatcherResponseAndOffset =
        new DispatcherResponseAndOffset(DispatcherResponse.Code.COMMIT, 10);
    sameDispatcherResponseAndOffset =
        new DispatcherResponseAndOffset(DispatcherResponse.Code.COMMIT, 10);
    differentDispatcherResponseAndOffset =
        new DispatcherResponseAndOffset(DispatcherResponse.Code.DLQ);
  }

  @Test
  public void testGetCode() {
    Assert.assertEquals(DispatcherResponse.Code.COMMIT, dispatcherResponseAndOffset.getCode());
  }

  @Test
  public void testGetOffset() {
    Assert.assertEquals(10, dispatcherResponseAndOffset.getOffset());
  }

  @Test
  public void testEquals() {
    Assert.assertFalse(dispatcherResponseAndOffset.equals(null));
    Assert.assertFalse(dispatcherResponseAndOffset.equals(new Object()));
    Assert.assertEquals(sameDispatcherResponseAndOffset, dispatcherResponseAndOffset);
    Assert.assertNotEquals(differentDispatcherResponseAndOffset, dispatcherResponseAndOffset);
  }

  @Test
  public void testHashCode() {
    Assert.assertEquals(
        sameDispatcherResponseAndOffset.hashCode(), dispatcherResponseAndOffset.hashCode());
    Assert.assertNotEquals(
        differentDispatcherResponseAndOffset.hashCode(), dispatcherResponseAndOffset.hashCode());
  }
}
