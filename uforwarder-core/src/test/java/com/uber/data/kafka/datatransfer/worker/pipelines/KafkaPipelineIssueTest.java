package com.uber.data.kafka.datatransfer.worker.pipelines;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class KafkaPipelineIssueTest {
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
    Assertions.assertEquals(expected, actual);
  }
}
