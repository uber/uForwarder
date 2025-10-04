package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.ScaleStateSnapshot;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ScaleStateTest {
  private TestUtils.TestTicker testTicker;
  private AutoScalarConfiguration config;
  private ScaleState.Builder builder;

  @BeforeEach
  public void setup() {
    testTicker = new TestUtils.TestTicker();
    config = new AutoScalarConfiguration();
    config.setDryRun(false);
    config.setHibernatingEnabled(true);
    config.setMessagesPerSecPerWorker(2000);
    config.setJobStatusTTL(Duration.ofDays(2));
    config.setUpScaleMaxFactor(1.4);
    config.setUpScaleMinFactor(1.1);
    config.setDownScaleMaxFactor(0.9);
    config.setDownScaleMinFactor(0.8);
    builder = ScaleState.newBuilder().withConfig(config).withTicker(testTicker);
  }

  @Test
  public void testScaleUp() {
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2, state.getScale(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      state = state.onSample(i < 28 ? 2 : 4);
    }
    Assertions.assertEquals(2.792, state.getScale(), 0.001);
  }

  @Test
  public void testScaleUpBelowMinFactor() {
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2, state.getScale(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      state = state.onSample(2.05);
    }
    Assertions.assertEquals(2, state.getScale(), 0.001);
  }

  @Test
  public void testResetAndScaleUp() {
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2, state.getScale(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      state = state.onSample(2.05);
    }
    Assertions.assertEquals(2, state.getScale(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      state = state.onSample(i < 28 ? 2 : 4);
    }
    Assertions.assertEquals(2.792, state.getScale(), 0.001);
  }

  @Test
  public void testScaleUp20Percent() {
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2, state.getScale(), 0.001);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      state = state.onSample(i < 28 ? 2 : 2.4);
    }
    Assertions.assertEquals(2.4, state.getScale(), 0.001);
  }

  @Test
  public void testScaleDownBelowMin() {
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2, state.getScale(), 0.001);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      state = state.onSample(i < samples - 10 ? 0.5 : 2);
    }
    Assertions.assertEquals(1.6, state.getScale(), 0.001);
  }

  @Test
  public void testScaleDownAboveMax() {
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2, state.getScale(), 0.001);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      state = state.onSample(1.95);
    }
    Assertions.assertEquals(2, state.getScale(), 0.001);
  }

  @Test
  public void testScaleDown15Percent() {
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2, state.getScale(), 0.001);
    int samples = 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofSeconds(60));
      state = state.onSample(1.7);
    }
    Assertions.assertEquals(1.7, state.getScale(), 0.01);
  }

  @Test
  public void testHibernatingAndBootstrap() {
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2, state.getScale(), 0.001);
    int samples = 3 * 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofMinutes(1));
      state = state.onSample(0.0);
    }
    Assertions.assertEquals(0.0, state.getScale(), 0.01);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      state = state.onSample(0.005);
    }
    Assertions.assertEquals(0.01, state.getScale(), 0.0001);
  }

  @Test
  public void testHibernating() {
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2, state.getScale(), 0.001);
    int samples = 3 * 24 * 60 + 1;
    for (int i = 0; i < samples; ++i) {
      testTicker.add(Duration.ofMinutes(1));
      state = state.onSample(0.0);
    }
    Assertions.assertEquals(0.0, state.getScale(), 0.01);
  }

  @Test
  public void testBootstrap() {
    ScaleState state = builder.build(0.0, false);
    Assertions.assertEquals(0.0, state.getScale(), 0.01);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      state = state.onSample(0.5);
    }
    Assertions.assertEquals(0.5, state.getScale(), 0.0001);
  }

  @Test
  public void testResetAndBootstrap() {
    // stay in hibernating state for a while then bootstrap
    ScaleState state = builder.build(0.0, false);
    Assertions.assertEquals(0.0, state.getScale(), 0.01);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      state = state.onSample(0.0);
    }
    Assertions.assertEquals(0.0, state.getScale(), 0.01);
    for (int i = 0; i < 61; ++i) {
      testTicker.add(Duration.ofSeconds(5));
      state = state.onSample(0.5);
    }
    Assertions.assertEquals(0.5, state.getScale(), 0.0001);
  }

  @Test
  public void testSnapshotRunningState() {
    ScaleState state = builder.build(2.0, false);
    ScaleStateSnapshot scaleStateSnapshot = state.snapshot();
    Assertions.assertEquals(3, scaleStateSnapshot.getScaleComputerSnapshotsList().size());
    Assertions.assertEquals(
        2.0d,
        scaleStateSnapshot
            .getScaleComputerSnapshotsList()
            .get(0)
            .getWindowedComputerSnapshot()
            .getBaseScale(),
        0.000001);
    Assertions.assertEquals(2.0, scaleStateSnapshot.getScale(), 0.0001);
  }

  @Test
  public void testSnapshotHibernateState() {
    ScaleState state = builder.build(0.0, false);
    ScaleStateSnapshot scaleStateSnapshot = state.snapshot();
    Assertions.assertEquals(1, scaleStateSnapshot.getScaleComputerSnapshotsList().size());
    Assertions.assertEquals(
        1.0d,
        scaleStateSnapshot
            .getScaleComputerSnapshotsList()
            .get(0)
            .getWindowedComputerSnapshot()
            .getBaseScale(),
        0.000001);
    Assertions.assertEquals(0.0, scaleStateSnapshot.getScale(), 0.0001);
  }

  @Test
  public void testNoopScaleComputer() {
    ScaleComputer scaleComputer = ScaleComputer.NOOP;
    Assertions.assertEquals(Optional.empty(), scaleComputer.onSample(0.0));
    Assertions.assertNotNull(scaleComputer.snapshot());
  }

  @Test
  public void testBuildStateWithJitter() {
    AutoScalarConfiguration config = new AutoScalarConfiguration();
    config.setMaxScaleWindowDurationJitter(0.5);
    builder = ScaleState.newBuilder().withConfig(config).withTicker(testTicker);
    ScaleState state = builder.build(2.0, false);
    Assertions.assertEquals(2.0, state.getScale(), 0.01);
  }
}
