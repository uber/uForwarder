package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CancelResultTest {

  @Test
  public void testClose() {
    CancelResult cancelResult = new CancelResult(DispatcherResponse.Code.RETRY);
    Assertions.assertTrue(cancelResult.close(true));
    Assertions.assertFalse(cancelResult.close(true));
  }

  @Test
  public void testCloseErrorResult() {
    CancelResult cancelResult = new CancelResult(CancelResult.ErrorCode.RATE_LIMITED);
    Assertions.assertTrue(cancelResult.close(true));
    Assertions.assertFalse(cancelResult.close(true));
  }
}
