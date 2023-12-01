package com.uber.data.kafka.datatransfer.management;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.controller.coordinator.LeaderSelector;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class MastersJsonTest extends FievelTestBase {
  @Test
  public void testRead() throws Exception {
    LeaderSelector leaderSelector = Mockito.mock(LeaderSelector.class);
    Mockito.when(leaderSelector.getLeaderId()).thenReturn("leader:1234");
    Mockito.when(leaderSelector.getFollowers()).thenReturn(ImmutableList.of("follower:5678"));
    Assert.assertNotNull(new MastersJson(leaderSelector).read());
  }
}
