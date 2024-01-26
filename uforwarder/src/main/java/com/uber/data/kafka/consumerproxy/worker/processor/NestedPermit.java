package com.uber.data.kafka.consumerproxy.worker.processor;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.consumerproxy.worker.limiter.InflightLimiter;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Composition of permits */
class NestedPermit implements InflightLimiter.Permit {
  private AtomicBoolean completed;
  private final List<InflightLimiter.Permit> permits;
  private final List<Consumer> resultConsumers;

  /**
   * Creates a nested permit with nested permits
   *
   * @param permits
   */
  NestedPermit(InflightLimiter.Permit... permits) {
    this.permits = ImmutableList.copyOf(permits);
    this.completed = new AtomicBoolean(false);
    this.resultConsumers = ImmutableList.of();
  }

  NestedPermit(List<InflightLimiter.Permit> permits, List<Consumer> resultConsumers) {
    this.permits = permits;
    this.completed = new AtomicBoolean(false);
    this.resultConsumers = resultConsumers;
  }

  /**
   * Completes the permit
   *
   * @param result the result
   * @return true if the permit is completed
   */
  @Override
  public boolean complete(InflightLimiter.Result result) {
    if (completed.compareAndSet(false, true)) {
      permits.stream().forEach(permit -> permit.complete(result));
      resultConsumers.stream().forEach(consumer -> consumer.accept(result));
      return true;
    }
    return false;
  }
}
