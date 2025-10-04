package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PipelineHealthStateTest {
  private TestUtils.TestTicker ticker;

  private PipelineHealthState pipelineHealthState;

  private int windowSizeSeconds = 10;
  private int windowCount = 3;

  private PipelineHealthIssue issue1 = new PipelineHealthIssue("test-issue-1");
  private PipelineHealthIssue issue2 = new PipelineHealthIssue("test-issue-2");

  @BeforeEach
  public void setUp() {
    ticker = new TestUtils.TestTicker();
    pipelineHealthState =
        new PipelineHealthState(ticker, Duration.ofSeconds(windowSizeSeconds), windowCount);
  }

  @Test
  public void testRecordIssue() {
    // Record the issue
    pipelineHealthState.recordIssue(issue1);
    Assertions.assertEquals(pipelineHealthState.getIssues(), Set.of(issue1));
  }

  @Test
  public void testWindowRecord() {
    PipelineHealthState.MutableHealthStateWindow window =
        pipelineHealthState.new MutableHealthStateWindow();
    window.recordIssue(issue1);
    window.recordIssue(issue2);
    Set<PipelineHealthIssue> issues = window.getIssues();
    Assertions.assertEquals(issues, Set.of(issue1, issue2));
  }
}
