package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class KafkaPipelineIssueTest extends FievelTestBase {
  @Test
  public void testGetValue() {
    Set<Integer> expected = Set.of(1 << 0, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7);
    Set<Integer> actual = new HashSet<>();
    for (KafkaPipelineIssue issue : KafkaPipelineIssue.values()) {
      actual.add(issue.getPipelineHealthIssue().getValue());
    }
    Assert.assertEquals(expected, actual);
  }
}
