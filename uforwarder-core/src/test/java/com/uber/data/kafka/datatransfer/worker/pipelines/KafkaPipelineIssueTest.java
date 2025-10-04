package com.uber.data.kafka.datatransfer.worker.pipelines;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class KafkaPipelineIssueTest extends FievelTestBase {
  @Test
  public void testGetValue() {
    Set<String> expected =
        Set.of(
            "message-rate-limited",
            "bytes-rate-limited",
            "inflight-message-limited",
            "permission-denied",
            "invalid-response-received",
            "retry-without-retry-queue",
            "median-rpc-latency-high",
            "max-rpc-latency-high");
    Set<String> actual = new HashSet<>();
    for (KafkaPipelineIssue issue : KafkaPipelineIssue.values()) {
      actual.add(issue.getPipelineHealthIssue().getName());
    }
    Assert.assertEquals(expected, actual);
  }
}
