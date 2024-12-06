package com.uber.data.kafka.consumerproxy.worker.limiter;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * * LongFixedInflightLimiter with a bootstrap phase During bootstrap phase, inflight is limited by
 * bootstrap limit as well when the number of completed tasks exceeded defined threshold (bootstrap
 * limit * percent), it will exit bootstrap phase and enter working phase During working phase, the
 * behavior is same as {@link LongFixedInflightLimiter}
 */
public final class BootstrapLongFixedInflightLimiter extends LongFixedInflightLimiter {
  private final LongFixedInflightLimiter bootstrapLimiter;
  private final AtomicReference<State> stateReference;
  private final long bootstrapCompleteThreshold;

  /**
   * Instantiates an new instance new instance will be in bootstrap phase
   *
   * @param builder the builder
   */
  public BootstrapLongFixedInflightLimiter(Builder builder) {
    super(builder.limit);
    bootstrapCompleteThreshold = builder.bootstrapCompleteThreshold;
    bootstrapLimiter = new LongFixedInflightLimiter(builder.bootstrapLimit);
    stateReference = new AtomicReference<>(new BootstrapState());
  }

  /**
   * Creates new builder
   *
   * @return the builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  @Nullable
  Permit doAcquire(AcquireMode acquireMode, int n) throws InterruptedException {
    State state = stateReference.get();
    if (state == null) {
      throw new IllegalStateException("state should not be null");
    }
    return state.doAcquire(acquireMode, n);
  }

  /**
   * Tests if it's in bootstrap phase
   *
   * @return the boolean
   */
  @VisibleForTesting
  protected boolean isBootstrapping() {
    State state = stateReference.get();
    if (state != null) {
      return state instanceof BootstrapState;
    }
    return false;
  }

  private interface State {

    /**
     * Do acquire a permit.
     *
     * @param acquireMode the acquire mode
     * @param n the n
     * @return the permit
     * @throws InterruptedException the interrupted exception
     */
    @Nullable
    Permit doAcquire(AcquireMode acquireMode, int n) throws InterruptedException;
  }

  private class BootstrapState implements State {
    private final AtomicLong completed = new AtomicLong();

    @Nullable
    @Override
    public Permit doAcquire(AcquireMode acquireMode, int n) throws InterruptedException {
      Permit permit = BootstrapLongFixedInflightLimiter.super.doAcquire(acquireMode, n);
      if (permit == null) {
        return null;
      }

      Permit bootstrapPermit = bootstrapLimiter.doAcquire(acquireMode, n);
      if (bootstrapPermit == null) {
        // release acquired permit to avoid leak
        permit.complete();
        return null;
      }

      return new BootstrapPermit(permit, bootstrapPermit);
    }

    private class BootstrapPermit implements Permit {
      private final Permit[] permits;

      /**
       * Instantiates a new permit.
       *
       * @param permits the permits
       */
      BootstrapPermit(Permit... permits) {
        this.permits = permits;
      }

      @Override
      public boolean complete(Result result) {
        boolean ret = false;
        for (Permit permit : permits) {
          ret = permit.complete(result);
        }
        // switch to working state when processed count > threshold
        if (ret && result == Result.Succeed) {
          if (completed.incrementAndGet() > bootstrapCompleteThreshold) {
            stateReference.compareAndSet(BootstrapState.this, new WorkingState());
          }
        }

        return ret;
      }
    }
  }

  private class WorkingState implements State {
    @Nullable
    @Override
    public Permit doAcquire(AcquireMode acquireMode, int n) throws InterruptedException {
      return BootstrapLongFixedInflightLimiter.super.doAcquire(acquireMode, n);
    }
  }

  /** Builder of BootstrapLongFixedInflightLimiter */
  public static class Builder {
    private static final long BOOTSTRAP_LIMIT = 100;
    private static final long BOOTSTRAP_COMPLETE_THRESHOLD = 100;
    private static final long LIMIT = 0;
    private long bootstrapLimit = BOOTSTRAP_LIMIT;
    private long bootstrapCompleteThreshold = BOOTSTRAP_COMPLETE_THRESHOLD;
    private long limit = LIMIT;

    /**
     * Sets bootstrap limit.
     *
     * @param bootstrapLimit the bootstrap limit
     * @return the builder
     */
    public Builder withBootstrapLimit(long bootstrapLimit) {
      this.bootstrapLimit = bootstrapLimit;
      return this;
    }

    /**
     * Sets bootstrap complete threshold. switch to working state when processed count > the
     * threshold
     *
     * @param bootstrapCompleteThreshold the bootstrap compete threshold
     * @return the builder
     */
    public Builder withBootstrapCompleteThreshold(long bootstrapCompleteThreshold) {
      this.bootstrapCompleteThreshold = bootstrapCompleteThreshold;
      return this;
    }

    /**
     * Sets initial limit
     *
     * @param limit the limit
     * @return the builder
     */
    public Builder withLimit(long limit) {
      this.limit = limit;
      return this;
    }

    /**
     * Builds limiter
     *
     * @return the long fixed inflight limiter
     */
    public BootstrapLongFixedInflightLimiter build() {
      return new BootstrapLongFixedInflightLimiter(this);
    }
  }
}
