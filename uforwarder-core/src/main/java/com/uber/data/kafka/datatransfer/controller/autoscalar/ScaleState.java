package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.datatransfer.ScaleComputerSnapshot;
import com.uber.data.kafka.datatransfer.ScaleStateSnapshot;
import com.uber.data.kafka.datatransfer.WindowedComputerSnapshot;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/** ScalarState indicates in-memory autoscalar state of workload */
@ThreadSafe
abstract class ScaleState {
  private static final double ONE = 1.0d;
  private static final double EPSILON = 0.0000001;
  private static final double BOOTSTRAP_SCALE =
      1.0d; // max scale a hibernating job can bootstrap to

  /** The ScalarState Builder. */
  protected final Builder builder;

  private final double scale;

  private ScaleState(Builder builder, double scale) {
    this.builder = builder;
    this.scale = scale;
  }

  /**
   * Gets current scale of work load
   *
   * @return the scale
   */
  public double getScale() {
    return this.scale;
  }

  /**
   * Takes a dump of internal computer state
   *
   * @return the dump of internal state
   */
  public abstract ScaleStateSnapshot snapshot();

  /**
   * Takes sample scale of work load
   *
   * @param scale the sample scale
   * @return the next scalar state
   */
  public abstract ScaleState onSample(double scale);

  /**
   * creates builder.
   *
   * @return the builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /** ScalarState Builder */
  public static class Builder {
    private AutoScalarConfiguration config = new AutoScalarConfiguration();
    private ScaleWindowManager scaleWindowManager = new ScaleWindowManager();
    private Ticker ticker = Ticker.systemTicker();

    /**
     * Sets autoscalar config
     *
     * @param config the autoscalar config
     * @return the builder
     */
    Builder withConfig(AutoScalarConfiguration config) {
      this.config = config;
      return this;
    }

    Builder withWindowManager(ScaleWindowManager scaleWindowManager) {
      this.scaleWindowManager = scaleWindowManager;
      return this;
    }

    /**
     * Sets ticker
     *
     * @param ticker the ticker
     * @return the builder
     */
    Builder withTicker(Ticker ticker) {
      this.ticker = ticker;
      return this;
    }

    /**
     * Builds scalar state.
     *
     * @param scale the current scale
     * @return the scalar state
     */
    public ScaleState build(double scale) {
      if (scale == Scalar.ZERO) {
        return new HibernateState(this);
      } else {
        return new RunningState(this, scale);
      }
    }
  }

  /**
   * Normal working state of work load that can be scale up, down or enter hibernation when enabled
   */
  private static class RunningState extends ScaleState {
    // Contains upScaleComputer and downScaleComputer
    private final List<ScaleComputer> upDownScaleComputers;
    private final ScaleComputer hibernatingComputer;

    private RunningState(Builder builder, double scale) {
      this(builder, scale, buildHibernatingComputer(builder, scale));
    }

    /**
     * Instantiates a new Running state.
     *
     * @param builder the builder
     * @param scale the scale
     * @param hibernatingComputer the hibernating computer
     */
    RunningState(Builder builder, double scale, ScaleComputer hibernatingComputer) {
      super(builder, scale);
      this.hibernatingComputer = hibernatingComputer;
      // initialize window for up scale,down scale,zero scale
      ImmutableList.Builder<ScaleComputer> listBuilder = new ImmutableList.Builder();
      listBuilder.add(
          new WindowedScaleComputer(
              ScaleWindow.newBuilder()
                  .withWindowDurationSupplier(
                      () -> builder.scaleWindowManager.getUpScaleWindowDuration().toNanos())
                  .withTicker(builder.ticker),
              scale,
              ONE,
              builder.config.getUpScaleMaxFactor(),
              builder.config.getUpScaleMinFactor(),
              builder.config.getUpScaleMaxFactor(),
              builder.config.getUpScalePercentile()));
      // with small scale, stop further scale down
      if (scale > EPSILON) {
        listBuilder.add(
            new WindowedScaleComputer(
                ScaleWindow.newBuilder()
                    .withWindowDurationSupplier(
                        () -> builder.scaleWindowManager.getDownScaleWindowDuration().toNanos())
                    .withTicker(builder.ticker),
                scale,
                builder.config.getDownScaleMinFactor(),
                ONE,
                builder.config.getDownScaleMinFactor(),
                builder.config.getDownScaleMaxFactor(),
                builder.config.getDownScalePercentile()));
      }
      upDownScaleComputers = listBuilder.build();
    }

    @Override
    public ScaleStateSnapshot snapshot() {
      return ScaleStateSnapshot.newBuilder()
          .addAllScaleComputerSnapshots(
              upDownScaleComputers.stream()
                  .map(ScaleComputer::snapshot)
                  .collect(Collectors.toList()))
          .addScaleComputerSnapshots(hibernatingComputer.snapshot())
          .build();
    }

    private static ScaleComputer buildHibernatingComputer(Builder builder, double scale) {
      return builder.config.isHibernatingEnabled()
          ? new WindowedScaleComputer(
              ScaleWindow.newBuilder()
                  .withWindowDurationSupplier(
                      () -> builder.scaleWindowManager.getHibernateWindowDuration().toNanos())
                  .withTicker(builder.ticker),
              scale,
              Scalar.ZERO,
              ONE,
              Scalar.ZERO,
              Scalar.ZERO,
              builder.config.getDownScalePercentile())
          : ScaleComputer.NOOP;
    }

    @Override
    public ScaleState onSample(double scale) {
      for (ScaleComputer computer : upDownScaleComputers) {
        Optional<Double> nextScale = computer.onSample(scale);
        if (nextScale.isPresent()) {
          // reuse zeroScaleComputer
          return new RunningState(builder, nextScale.get(), hibernatingComputer);
        }
      }
      Optional<Double> nextScale = hibernatingComputer.onSample(scale);
      if (nextScale.isPresent()) {
        return new HibernateState(builder);
      }
      return this;
    }
  }

  /**
   * Hibernating state of work load that supports bootstrapping when scale went up from zero to
   * positive
   */
  private static class HibernateState extends ScaleState {
    private final ScaleComputer bootstrapComputer;

    private HibernateState(Builder builder) {
      super(builder, Scalar.ZERO);
      bootstrapComputer =
          new WindowedScaleComputer(
              ScaleWindow.newBuilder()
                  .withWindowDurationSupplier(
                      () -> builder.scaleWindowManager.getUpScaleWindowDuration().toNanos())
                  .withTicker(builder.ticker),
              BOOTSTRAP_SCALE,
              Scalar.ZERO,
              ONE,
              EPSILON,
              ONE,
              builder.config.getUpScalePercentile());
    }

    @Override
    public ScaleStateSnapshot snapshot() {
      return ScaleStateSnapshot.newBuilder()
          .addScaleComputerSnapshots(bootstrapComputer.snapshot())
          .build();
    }

    @Override
    public ScaleState onSample(double scale) {
      Optional<Double> nextScale = bootstrapComputer.onSample(scale);
      if (nextScale.isPresent()) {
        return new RunningState(builder, nextScale.get());
      }
      return this;
    }
  }

  private static class WindowedScaleComputer implements ScaleComputer {
    private final ScaleWindow.Builder windowBuilder;
    // function to get current scale
    private final double scale;
    // minimal/maximum factors to current scale, limits boundary of scale window
    // when sample fall out of min/max boundary, it will be ceil/floor into the boundary
    // but no sample will be ignored
    private final double minInputFactor;
    private final double maxInputFactor;
    // minimal/maximum factors to current scale, limits boundary of proposed scale
    private final double minOutputFactor;
    private final double maxOutputFactor;
    // percentile to get scale from window
    private final double windowPercentile;
    // function to update current scale
    private ScaleWindow scaleWindow;

    /**
     * Instantiates a new Windowed scale computer.
     *
     * @param windowBuilder the window builder
     * @param scale the scale
     * @param minInputFactor the minimal factor limits samples took to current scale
     * @param maxInputFactor the maximum factor limits samples took to current scale
     * @param minOutputFactor the minimal factor limits proposal to current scale
     * @param maxOutputFactor the maximum factor limits proposal to current scale
     * @param windowPercentile the window percentile to get proposal
     */
    WindowedScaleComputer(
        ScaleWindow.Builder windowBuilder,
        double scale,
        double minInputFactor,
        double maxInputFactor,
        double minOutputFactor,
        double maxOutputFactor,
        double windowPercentile) {
      this.windowBuilder = windowBuilder;
      this.scale = scale;
      this.minInputFactor = minInputFactor;
      this.maxInputFactor = maxInputFactor;
      this.minOutputFactor = minOutputFactor;
      this.maxOutputFactor = maxOutputFactor;
      this.windowPercentile = windowPercentile;
      resetWindow();
    }

    private void resetWindow() {
      scaleWindow = windowBuilder.build(scale * minInputFactor, scale * maxInputFactor);
    }

    @Override
    public synchronized Optional<Double> onSample(double sample) {
      scaleWindow.add(sample);
      if (scaleWindow.isMature()) {
        double proposal = scaleWindow.getByPercentile(windowPercentile);
        if (proposal >= scale * minOutputFactor && proposal <= scale * maxOutputFactor) {
          // accept proposal after validation
          return Optional.of(proposal);
        } else {
          // reset the tumbling window when proposal not accepted
          resetWindow();
        }
      }
      return Optional.empty();
    }

    @Override
    public ScaleComputerSnapshot snapshot() {
      return ScaleComputerSnapshot.newBuilder()
          .setWindowedComputerSnapshot(
              WindowedComputerSnapshot.newBuilder()
                  .setWindowSnapshot(scaleWindow.snapshot())
                  .setCurrentScale(scale)
                  .setPercentile(windowPercentile)
                  .setPercentileScale(scaleWindow.getByPercentile(windowPercentile))
                  .setLowerBoundary(scale * minOutputFactor)
                  .setUpperBoundary(scale * maxOutputFactor)
                  .build())
          .build();
    }
  }
}
