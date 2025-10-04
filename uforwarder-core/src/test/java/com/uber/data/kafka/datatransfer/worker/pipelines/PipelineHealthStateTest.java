package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PipelineHealthStateTest extends FievelTestBase {
  private TestUtils.TestTicker ticker;

  private PipelineHealthState pipelineHealthState;

  private int windowSizeSeconds = 10;
  private int windowCount = 3;

  @Before
  public void setUp() {
    ticker = new TestUtils.TestTicker();
    pipelineHealthState =
        new PipelineHealthState(ticker, Duration.ofSeconds(windowSizeSeconds), windowCount);
  }

  @Test
  public void testRecordIssue() {
    // Record the issue
    pipelineHealthState.recordIssue(PipelineHealthIssue.INFLIGHT_MESSAGE_LIMITED);
    Assert.assertEquals(
        PipelineHealthIssue.INFLIGHT_MESSAGE_LIMITED.value(), pipelineHealthState.getStateValue());
  }

  @Test
  public void testWindowRecord() {
    PipelineHealthState.MutableHealthStateWindow window =
        pipelineHealthState.new MutableHealthStateWindow();
    window.recordIssue(PipelineHealthIssue.INFLIGHT_MESSAGE_LIMITED);
    window.recordIssue(PipelineHealthIssue.PERMISSION_DENIED);
    int value = window.getValue();
    Assert.assertEquals(
        Set.of(PipelineHealthIssue.INFLIGHT_MESSAGE_LIMITED, PipelineHealthIssue.PERMISSION_DENIED),
        PipelineHealthIssue.decode(value));
  }
}
