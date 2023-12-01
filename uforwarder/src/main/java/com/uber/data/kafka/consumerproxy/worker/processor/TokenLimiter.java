package com.uber.data.kafka.consumerproxy.worker.processor;

/** TokenLimiter accepts token credits also limits number of tokens can be acquired from */
public interface TokenLimiter {
  TokenLimiter NOOP = new TokenLimiter() {};

  /**
   * Add specific number of tokens into the limiter
   *
   * @param n the number of tokens to credit
   */
  default void credit(int n) {}

  /**
   * try to acquire number of tokens from the limiter return true when number of token left >
   * minimalTokens else return false
   *
   * @param n the number of tokens to acquire
   */
  default boolean tryAcquire(int n) {
    return true;
  }

  /**
   * Gets metrics.
   *
   * @return the metrics
   */
  default Metrics getMetrics() {
    return Metrics.NOOP;
  }

  interface Metrics {
    Metrics NOOP = new Metrics() {};

    /**
     * Gets number of tokens in the limiter
     *
     * @return the num tokens
     */
    default int getNumTokens() {
      return Integer.MAX_VALUE;
    }
  }
}
