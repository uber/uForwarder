package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class PipelineHealthIssueTest extends FievelTestBase {
  @Test
  public void testGetValue() {
    Set<Integer> expected = Set.of(1 << 0, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6);
    Set<Integer> actual = new HashSet<>();
    for (PipelineHealthIssue issue : PipelineHealthIssue.values()) {
      actual.add(issue.value());
    }
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDecode() {
    Set<PipelineHealthIssue> issues = PipelineHealthIssue.decode(24);
    Assert.assertEquals(
        Set.of(
            PipelineHealthIssue.INVALID_RESPONSE_RECEIVED, PipelineHealthIssue.PERMISSION_DENIED),
        issues);
  }
}
