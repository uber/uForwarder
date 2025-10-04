package com.uber.data.kafka.datatransfer.worker.pipelines;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PipelineHealthIssueTest {
  @Test
  public void testGetValue() {
    PipelineHealthIssue issue1 = new PipelineHealthIssue("test-issue-1");
    Assertions.assertEquals("test-issue-1", issue1.getName());
  }

  @Test
  public void testEquals() {
    PipelineHealthIssue issue1 = new PipelineHealthIssue("test-issue");
    PipelineHealthIssue issue2 = new PipelineHealthIssue("test-issue");
    Assertions.assertEquals(issue1, issue2);
    Assertions.assertEquals(issue1.hashCode(), issue2.hashCode());
  }
}
