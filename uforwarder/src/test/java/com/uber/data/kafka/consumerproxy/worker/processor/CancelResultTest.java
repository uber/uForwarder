package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.consumerproxy.worker.dispatcher.DispatcherResponse;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class CancelResultTest extends FievelTestBase {

  @Test
  public void testClose() {
    CancelResult cancelResult = new CancelResult(DispatcherResponse.Code.RETRY);
    Assert.assertTrue(cancelResult.close(true));
    Assert.assertFalse(cancelResult.close(true));
  }

  @Test
  public void testCloseErrorResult() {
    CancelResult cancelResult = new CancelResult(CancelResult.ErrorCode.RATE_LIMITED);
    Assert.assertTrue(cancelResult.close(true));
    Assert.assertFalse(cancelResult.close(true));
  }
}
