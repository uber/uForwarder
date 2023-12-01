package com.uber.data.kafka.datatransfer.controller.coordinator;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.fievel.testing.base.FievelTestBase;
import java.io.IOException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class LeaderSelectorTest extends FievelTestBase {

  private LeaderSelector leaderSelector;
  private CuratorFramework curatorFramework;
  private LeaderLatch leaderLatch;

  @Before
  public void setup() {
    curatorFramework = Mockito.mock(CuratorFramework.class);
    leaderLatch = Mockito.mock(LeaderLatch.class);
    leaderSelector =
        new LeaderSelector(
            curatorFramework, leaderLatch, "/leader", "localhost:2111", CoreInfra.NOOP);
  }

  @Test
  public void testLeaderSelectorLifecycleCreateLeaderNode() throws Exception {
    LeaderLatchListener listener = Mockito.mock(LeaderLatchListener.class);
    Mockito.when(curatorFramework.getState())
        .thenReturn(CuratorFrameworkState.LATENT)
        .thenReturn(CuratorFrameworkState.STARTED)
        .thenReturn(CuratorFrameworkState.STARTED)
        .thenReturn(CuratorFrameworkState.LATENT);
    Mockito.when(leaderLatch.getState())
        .thenReturn(LeaderLatch.State.LATENT)
        .thenReturn(LeaderLatch.State.STARTED)
        .thenReturn(LeaderLatch.State.STARTED)
        .thenReturn(LeaderLatch.State.LATENT);
    ExistsBuilder existsBuilder = Mockito.mock(ExistsBuilder.class);
    CreateBuilder createBuilder = Mockito.mock(CreateBuilder.class);
    Mockito.when(curatorFramework.checkExists()).thenReturn(existsBuilder);
    Mockito.when(curatorFramework.create()).thenReturn(createBuilder);
    Mockito.when(existsBuilder.forPath("/leader")).thenReturn(null);

    try {
      leaderSelector.registerListener(listener);
      Assert.fail("registerListener should fail b/c leaderSelector is not started yet");
    } catch (IllegalStateException e) {
      Assert.assertEquals(
          "registerListener can't run before LeaderSelector started", e.getMessage());
    }

    leaderSelector.start();
    Mockito.verify(curatorFramework, Mockito.times(1)).start();
    Mockito.verify(leaderLatch, Mockito.times(1)).start();

    // start again should be OK
    leaderSelector.start();
    Mockito.verify(curatorFramework, Mockito.times(1)).start();
    Mockito.verify(leaderLatch, Mockito.times(1)).start();

    leaderSelector.registerListener(listener);
    ArgumentCaptor<LeaderLatchListener> listenerCaptor =
        ArgumentCaptor.forClass(LeaderLatchListener.class);
    // check that listener is registered and execute it.
    Mockito.verify(leaderLatch, Mockito.times(2)).addListener(listenerCaptor.capture());
    listenerCaptor.getValue().isLeader();
    listenerCaptor.getValue().notLeader();

    Participant participant = Mockito.mock(Participant.class);
    Mockito.when(leaderLatch.getLeader()).thenReturn(participant);
    leaderSelector.getLeaderId();
    Mockito.verify(participant, Mockito.times(1)).getId();

    leaderSelector.isLeader();
    Mockito.verify(leaderLatch, Mockito.times(1)).hasLeadership();

    leaderSelector.stop();
    Mockito.verify(curatorFramework, Mockito.times(1)).close();
    Mockito.verify(leaderLatch, Mockito.times(1)).close();

    // stop again should be OK
    leaderSelector.stop();
    Mockito.verify(curatorFramework, Mockito.times(1)).close();
    Mockito.verify(leaderLatch, Mockito.times(1)).close();
  }

  @Test
  public void testLeaderSelectorLifecycleExitingLeaderNode() throws Exception {
    LeaderLatchListener listener = Mockito.mock(LeaderLatchListener.class);
    Mockito.when(curatorFramework.getState())
        .thenReturn(CuratorFrameworkState.LATENT)
        .thenReturn(CuratorFrameworkState.STARTED)
        .thenReturn(CuratorFrameworkState.STARTED)
        .thenReturn(CuratorFrameworkState.LATENT);
    Mockito.when(leaderLatch.getState())
        .thenReturn(LeaderLatch.State.LATENT)
        .thenReturn(LeaderLatch.State.STARTED)
        .thenReturn(LeaderLatch.State.STARTED)
        .thenReturn(LeaderLatch.State.LATENT);
    ExistsBuilder existsBuilder = Mockito.mock(ExistsBuilder.class);
    Mockito.when(curatorFramework.checkExists()).thenReturn(existsBuilder);
    Mockito.when(existsBuilder.forPath("/leader")).thenReturn(new Stat());

    try {
      leaderSelector.registerListener(listener);
      Assert.fail("registerListener should fail b/c leaderSelector is not started yet");
    } catch (IllegalStateException e) {
      Assert.assertEquals(
          "registerListener can't run before LeaderSelector started", e.getMessage());
    }

    leaderSelector.start();
    Mockito.verify(curatorFramework, Mockito.times(1)).start();
    Mockito.verify(leaderLatch, Mockito.times(1)).start();

    // start again should be OK
    leaderSelector.start();
    Mockito.verify(curatorFramework, Mockito.times(1)).start();
    Mockito.verify(leaderLatch, Mockito.times(1)).start();

    leaderSelector.registerListener(listener);
    ArgumentCaptor<LeaderLatchListener> listenerCaptor =
        ArgumentCaptor.forClass(LeaderLatchListener.class);
    // check that listener is registered and execute it.
    Mockito.verify(leaderLatch, Mockito.times(2)).addListener(listenerCaptor.capture());
    listenerCaptor.getValue().isLeader();
    listenerCaptor.getValue().notLeader();

    Participant participant = Mockito.mock(Participant.class);
    Mockito.when(leaderLatch.getLeader()).thenReturn(participant);
    leaderSelector.getLeaderId();
    Mockito.verify(participant, Mockito.times(1)).getId();

    leaderSelector.isLeader();
    Mockito.verify(leaderLatch, Mockito.times(1)).hasLeadership();

    leaderSelector.stop();
    Mockito.verify(curatorFramework, Mockito.times(1)).close();
    Mockito.verify(leaderLatch, Mockito.times(1)).close();

    // stop again should be OK
    leaderSelector.stop();
    Mockito.verify(curatorFramework, Mockito.times(1)).close();
    Mockito.verify(leaderLatch, Mockito.times(1)).close();
  }

  @Test(expected = RuntimeException.class)
  public void testLeaderPathCreationException() throws Exception {
    Mockito.when(curatorFramework.getState())
        .thenReturn(CuratorFrameworkState.LATENT)
        .thenReturn(CuratorFrameworkState.STARTED);
    Mockito.when(leaderLatch.getState())
        .thenReturn(LeaderLatch.State.LATENT)
        .thenReturn(LeaderLatch.State.STARTED);
    ExistsBuilder existsBuilder = Mockito.mock(ExistsBuilder.class);
    CreateBuilder createBuilder = Mockito.mock(CreateBuilder.class);
    Mockito.when(curatorFramework.checkExists()).thenReturn(existsBuilder);
    Mockito.when(curatorFramework.create()).thenReturn(createBuilder);
    Mockito.doThrow(new RuntimeException()).when(existsBuilder).forPath("/leader");
    leaderSelector.start();
  }

  @Test(expected = RuntimeException.class)
  public void testLeaderSelectorStartException() throws Exception {
    Mockito.when(curatorFramework.getState())
        .thenReturn(CuratorFrameworkState.LATENT)
        .thenReturn(CuratorFrameworkState.STARTED);
    Mockito.when(leaderLatch.getState())
        .thenReturn(LeaderLatch.State.LATENT)
        .thenReturn(LeaderLatch.State.STARTED);
    ExistsBuilder existsBuilder = Mockito.mock(ExistsBuilder.class);
    CreateBuilder createBuilder = Mockito.mock(CreateBuilder.class);
    Mockito.when(curatorFramework.checkExists()).thenReturn(existsBuilder);
    Mockito.when(curatorFramework.create()).thenReturn(createBuilder);
    Mockito.when(curatorFramework.checkExists()).thenReturn(existsBuilder);
    Mockito.when(existsBuilder.forPath("/leader")).thenReturn(new Stat());
    Mockito.doThrow(new RuntimeException()).when(leaderLatch).start();
    leaderSelector.start();
  }

  @Test(expected = RuntimeException.class)
  public void testLeaderSelectorStopException() throws Exception {
    Mockito.when(leaderLatch.getState()).thenReturn(LeaderLatch.State.STARTED);
    Mockito.doThrow(new IOException()).when(leaderLatch).close();
    leaderSelector.stop();
  }

  @Test
  public void logAndMetrics() {
    leaderSelector.logAndMetrics();
  }

  @Test
  public void logAndMetricsNotLeader() {
    Mockito.when(leaderLatch.hasLeadership()).thenReturn(true);
    leaderSelector.logAndMetrics();
  }

  @Test
  public void testRunIfLeader() {
    Mockito.when(leaderLatch.hasLeadership()).thenReturn(true);
    Runnable runnable = Mockito.mock(Runnable.class);
    leaderSelector.runIfLeader("operationName", runnable);
    Mockito.verify(runnable, Mockito.times(1)).run();
  }

  @Test
  public void testRunIfLeaderOnFollower() {
    Mockito.when(leaderLatch.hasLeadership()).thenReturn(false);
    Runnable runnable = Mockito.mock(Runnable.class);
    leaderSelector.runIfLeader("operationName", runnable);
    Mockito.verify(runnable, Mockito.times(0)).run();
  }

  @Test
  public void testGetFollowers() throws Exception {
    Mockito.when(leaderLatch.getParticipants())
        .thenReturn(
            ImmutableList.of(
                new Participant("hostname:1234", true), new Participant("hostname:5678", false)));
    Assert.assertEquals(ImmutableList.of("hostname:5678"), leaderSelector.getFollowers());
  }
}
