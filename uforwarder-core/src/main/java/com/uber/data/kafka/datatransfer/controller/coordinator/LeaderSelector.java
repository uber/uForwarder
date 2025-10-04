package com.uber.data.kafka.datatransfer.controller.coordinator;

import com.google.common.annotations.VisibleForTesting;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.ZKUtils;
import com.uber.data.kafka.instrumentation.Instrumentation;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import net.logstash.logback.argument.StructuredArguments;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.annotation.Scheduled;

/** LeaderSelector wraps the curator LeaderLatch to support cluster leader election. */
public class LeaderSelector implements SmartLifecycle {
  private static final String ID = "id";
  private static final String ZK_PATH = "zk_path";
  private static final String METHOD = "method";

  private static final Logger logger = LoggerFactory.getLogger(LeaderSelector.class);

  private final LeaderLatch leaderLatch;
  private final String nodeId;
  private final CoreInfra infra;
  private final String latchPath;
  private final CuratorFramework curatorFramework;
  private final AtomicBoolean running;

  @VisibleForTesting
  LeaderSelector(
      CuratorFramework curatorFramework,
      LeaderLatch leaderLatch,
      String latchPath,
      String nodeId,
      CoreInfra infra) {
    this.curatorFramework = curatorFramework;
    this.leaderLatch = leaderLatch;
    this.latchPath = latchPath;
    this.nodeId = nodeId;
    this.infra = infra;
    this.running = new AtomicBoolean(false);
  }

  public static LeaderSelector of(
      String zkConnection, String latchPath, String nodeId, CoreInfra infra) {
    CuratorFramework curatorFramework = ZKUtils.getCuratorFramework(zkConnection);
    return new LeaderSelector(
        curatorFramework,
        new LeaderLatch(curatorFramework, latchPath, nodeId, LeaderLatch.CloseMode.NOTIFY_LEADER),
        latchPath,
        nodeId,
        infra);
  }

  @Override
  public void start() {
    if (curatorFramework.getState() == CuratorFrameworkState.LATENT) {
      curatorFramework.start();
      logger.info("leader.selector.client.started", StructuredArguments.keyValue(ID, nodeId));

      try {
        if (curatorFramework.checkExists().forPath(latchPath) == null) {
          curatorFramework.create().forPath(latchPath);
          logger.info(
              "leader.selector.path.created", StructuredArguments.keyValue(ZK_PATH, latchPath));
        }
      } catch (Exception e) {
        logger.error(
            "leaderselector failed to create latch path",
            StructuredArguments.keyValue(ZK_PATH, latchPath),
            e);
        throw new RuntimeException(e);
      }
    }
    if (leaderLatch.getState() == LeaderLatch.State.LATENT) {
      leaderLatch.addListener(
          new LeaderLatchListener() {
            @Override
            public void isLeader() {
              infra.scope().gauge(MetricsNames.IS_LEADER).update(1);
              logger.info(
                  "leader.selector.leader.acquired", StructuredArguments.keyValue(ID, nodeId));
            }

            @Override
            public void notLeader() {
              infra.scope().gauge(MetricsNames.IS_LEADER).update(0);
              logger.info(
                  "leader.selector.leader.notLeader", StructuredArguments.keyValue(ID, nodeId));
            }
          });
      try {
        leaderLatch.start();
        logger.info("leader.selector.latch.started", StructuredArguments.keyValue(ID, nodeId));
        infra.scope().gauge(MetricsNames.LATCH_STARTED).update(1);
      } catch (Exception e) {
        logger.error("trying to start leaderLatch failed", e);
        infra.scope().gauge(MetricsNames.LATCH_FAILED).update(1);
        throw new RuntimeException(e);
      }
    }
    running.set(true);
  }

  @Override
  public void stop() {

    if (leaderLatch.getState() == LeaderLatch.State.STARTED) {
      try {
        leaderLatch.close();
      } catch (IOException e) {
        logger.error("trying to close leaderLatch failed", e);
        throw new RuntimeException(e);
      }
    }

    if (curatorFramework.getState() == CuratorFrameworkState.STARTED) {
      curatorFramework.close();
    }

    running.set(false);
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  /**
   * Gets the id for current leader
   *
   * @return leader node id
   */
  public String getLeaderId() {
    // TODO(T4577053) run a background thread that checks the leader node view from each master and
    // reports to m3
    return Instrumentation.instrument.withRuntimeException(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> leaderLatch.getLeader().getId(),
        "leader.selector.getLeaderId");
  }

  public void registerListener(LeaderLatchListener leaderChangeListener) {
    if (!isRunning()) {
      logger.warn("registerListener can't run before LeaderSelector started");
      throw new IllegalStateException("registerListener can't run before LeaderSelector started");
    }
    leaderLatch.addListener(leaderChangeListener);
    logger.info(
        "Listener registered on LeaderSelector",
        StructuredArguments.keyValue(METHOD, leaderChangeListener.getClass().getSimpleName()));
  }

  /**
   * Gets the boolean indicate current instance is leader or not
   *
   * @return is leader nor not
   */
  public boolean isLeader() {
    return Instrumentation.instrument.withRuntimeException(
        logger,
        infra.scope(),
        infra.tracer(),
        () -> leaderLatch.hasLeadership(),
        "leader.selector.isLeader");
  }

  /** Runs the provided runnable on the leader. */
  public void runIfLeader(String runnableName, Runnable runnable) {
    if (!isLeader()) {
      String instrumentationName = "leader.selector.skip." + runnableName;
      logger.info(instrumentationName);
      infra.scope().counter(instrumentationName).inc(1);
      return;
    }
    runnable.run();
  }

  @Scheduled(fixedDelayString = "${master.coordinator.leaderSelector.metricsInterval}")
  public void logAndMetrics() {
    boolean isLeader = isLeader();
    if (isLeader) {
      infra.scope().gauge(MetricsNames.IS_LEADER).update(1);
      logger.debug("leader.selector.leader", StructuredArguments.keyValue(ID, nodeId));
    } else {
      infra.scope().gauge(MetricsNames.IS_LEADER).update(0);
      logger.debug("leader.selector.notLeader", StructuredArguments.keyValue(ID, nodeId));
    }
  }

  /** Returns the (non-leader) follower masters host:ports. */
  public List<String> getFollowers() {
    return Instrumentation.instrument.withRuntimeException(
        logger,
        infra.scope(),
        infra.tracer(),
        () ->
            leaderLatch.getParticipants().stream()
                .filter(p -> !p.isLeader())
                .map(Participant::getId)
                .collect(Collectors.toList()),
        "leader.selector.getFollowers");
  }

  private static class MetricsNames {
    static final String LATCH_STARTED = "leader.latch.started";
    static final String LATCH_FAILED = "leader.latch.failed";
    static final String IS_LEADER = "leader.selector.leader";
  }
}
