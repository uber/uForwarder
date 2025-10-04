package com.uber.data.kafka.datatransfer.management;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class MastersJsonTest {
  @Test
  public void testRead() throws Exception {
    LeaderSelector leaderSelector = Mockito.mock(LeaderSelector.class);
    Mockito.when(leaderSelector.getLeaderId()).thenReturn("leader:1234");
    Mockito.when(leaderSelector.getFollowers()).thenReturn(ImmutableList.of("follower:5678"));
    Assertions.assertNotNull(new MastersJson(leaderSelector).read());
  }
}
