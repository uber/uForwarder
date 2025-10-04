package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;

public class PipelineHealthIssueTest extends FievelTestBase {
  @Test
  public void testGetValue() {
    PipelineHealthIssue issue1 = new PipelineHealthIssue(4);
    Assert.assertEquals(16, issue1.getValue());
  }

  @Test
  public void testEquals() {
    PipelineHealthIssue issue1 = new PipelineHealthIssue(4);
    PipelineHealthIssue issue2 = new PipelineHealthIssue(4);
    Assert.assertEquals(issue1, issue2);
    Assert.assertEquals(issue1.hashCode(), issue2.hashCode());
  }
}
