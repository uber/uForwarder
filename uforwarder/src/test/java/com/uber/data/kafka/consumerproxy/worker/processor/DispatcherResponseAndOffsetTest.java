package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DispatcherResponseAndOffsetTest {
  DispatcherResponseAndOffset dispatcherResponseAndOffset;
  DispatcherResponseAndOffset sameDispatcherResponseAndOffset;
  DispatcherResponseAndOffset differentDispatcherResponseAndOffset;

  @BeforeEach
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
    Assertions.assertEquals(DispatcherResponse.Code.COMMIT, dispatcherResponseAndOffset.getCode());
  }

  @Test
  public void testGetOffset() {
    Assertions.assertEquals(10, dispatcherResponseAndOffset.getOffset());
  }

  @Test
  public void testEquals() {
    Assertions.assertFalse(dispatcherResponseAndOffset.equals(null));
    Assertions.assertFalse(dispatcherResponseAndOffset.equals(new Object()));
    Assertions.assertEquals(sameDispatcherResponseAndOffset, dispatcherResponseAndOffset);
    Assertions.assertNotEquals(differentDispatcherResponseAndOffset, dispatcherResponseAndOffset);
  }

  @Test
  public void testHashCode() {
    Assertions.assertEquals(
        sameDispatcherResponseAndOffset.hashCode(), dispatcherResponseAndOffset.hashCode());
    Assertions.assertNotEquals(
        differentDispatcherResponseAndOffset.hashCode(), dispatcherResponseAndOffset.hashCode());
  }
}
