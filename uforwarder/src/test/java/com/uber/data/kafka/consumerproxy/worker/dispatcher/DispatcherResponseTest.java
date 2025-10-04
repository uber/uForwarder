package com.uber.data.kafka.consumerproxy.worker.dispatcher;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DispatcherResponseTest {
  private DispatcherResponse responseCommit;
  private DispatcherResponse responseDLQ;

  @BeforeEach
  public void setup() {
    responseCommit = new DispatcherResponse(DispatcherResponse.Code.COMMIT);
    responseDLQ = new DispatcherResponse(DispatcherResponse.Code.DLQ);
  }

  @Test
  public void testGetCode() {
    Assertions.assertEquals(DispatcherResponse.Code.COMMIT, responseCommit.getCode());
  }

  @Test
  public void testEquals() {
    Assertions.assertEquals(responseCommit, responseCommit);
    Assertions.assertNotEquals(responseCommit, responseDLQ);
    Assertions.assertNotEquals(responseCommit, null);
    Assertions.assertNotEquals(null, responseCommit);
    Assertions.assertNotEquals(responseCommit, new Object());
    Assertions.assertNotEquals(new Object(), responseCommit);
  }

  @Test
  public void testHashCode() {
    Assertions.assertEquals(responseCommit.hashCode(), responseCommit.hashCode());
    Assertions.assertNotEquals(responseCommit.hashCode(), responseDLQ.hashCode());
  }

  @Test
  public void testCodeValue() {
    Assertions.assertEquals(0, DispatcherResponse.Code.DLQ.ordinal());
    Assertions.assertEquals(1, DispatcherResponse.Code.RETRY.ordinal());
    Assertions.assertEquals(2, DispatcherResponse.Code.RESQ.ordinal());
    Assertions.assertEquals(3, DispatcherResponse.Code.BACKOFF.ordinal());
    Assertions.assertEquals(4, DispatcherResponse.Code.DROPPED.ordinal());
    Assertions.assertEquals(5, DispatcherResponse.Code.INVALID.ordinal());
    Assertions.assertEquals(6, DispatcherResponse.Code.SKIP.ordinal());
    Assertions.assertEquals(7, DispatcherResponse.Code.COMMIT.ordinal());
  }
}
