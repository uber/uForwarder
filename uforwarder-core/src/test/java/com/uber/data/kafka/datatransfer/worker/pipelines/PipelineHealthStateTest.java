package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PipelineHealthStateTest extends FievelTestBase {
  private TestUtils.TestTicker ticker;

  private PipelineHealthState pipelineHealthState;

  private int windowSizeSeconds = 10;
  private int windowCount = 3;

  private PipelineHealthIssue issue1 = new PipelineHealthIssue(0);
  private PipelineHealthIssue issue2 = new PipelineHealthIssue(1);

  @Before
  public void setUp() {
    ticker = new TestUtils.TestTicker();
    pipelineHealthState =
        new PipelineHealthState(ticker, Duration.ofSeconds(windowSizeSeconds), windowCount);
  }

  @Test
  public void testRecordIssue() {
    // Record the issue
    pipelineHealthState.recordIssue(issue1);
    Assert.assertEquals(pipelineHealthState.getStateValue(), issue1.getValue());
  }

  @Test
  public void testWindowRecord() {
    PipelineHealthState.MutableHealthStateWindow window =
        pipelineHealthState.new MutableHealthStateWindow();
    window.recordIssue(issue1);
    window.recordIssue(issue2);
    int value = window.getValue();
    Assert.assertEquals(value, issue1.getValue() + issue2.getValue());
  }
}
