package com.uber.data.kafka.datatransfer.common;

import java.util.Map;

/** Data-source of dynamic configuration */
public interface DynamicConfiguration {

  /** DEFAULT configuration */
  DynamicConfiguration DEFAULT =
      new DynamicConfiguration() {
        public static final boolean REBALANCER_IS_OFFSET_COMMITTING_DEFAULT_VALUE = true;
        public static final boolean IS_HEADER_GLOBALLY_ALLOWED_DEFAULT_VALUE = false;
        public static final boolean IS_NETTY_ENABLED_DEFAULT_VALUE = false;
        public static final boolean IS_ZONE_ISOLATION_DISABLED_DEFAULT_VALUE = false;
        public static final boolean IS_AUTH_CLIENT_INTERCEPTOR_ENABLED_DEFAULT_VALUE = false;

        @Override
        public boolean isOffsetCommittingEnabled() {
          return REBALANCER_IS_OFFSET_COMMITTING_DEFAULT_VALUE;
        }

        @Override
        public boolean isHeaderAllowed(Map<String, String> constraints) {
          return IS_HEADER_GLOBALLY_ALLOWED_DEFAULT_VALUE;
        }

        @Override
        public boolean isNettyEnabled(Map<String, String> constraints) {
          return IS_NETTY_ENABLED_DEFAULT_VALUE;
        }

        @Override
        public boolean isZoneIsolationDisabled() {
          return IS_ZONE_ISOLATION_DISABLED_DEFAULT_VALUE;
        }

        @Override
        public boolean isAuthClientInterceptorEnabled(Map<String, String> constraints) {
          return IS_AUTH_CLIENT_INTERCEPTOR_ENABLED_DEFAULT_VALUE;
        }
      };

  /**
   * isOffsetCommittingEnabled checks if offset committing should be enabled
   *
   * @return boolean indicating if offset committing should be enabled, default to true
   */
  boolean isOffsetCommittingEnabled();

  /**
   * isHeaderAllowed checks if a header should be propagated
   *
   * @param constraints the set of constraints to be considered
   * @return boolean indicating if the concerned header should be propagated, default to false
   */
  boolean isHeaderAllowed(Map<String, String> constraints);

  /**
   * isNettyEnabled checks if dispatcher should be using netty channel builder.
   *
   * @param constraints the set of constraints to be considered
   * @return boolean indicating if the channel should use netty, default to false
   */
  boolean isNettyEnabled(Map<String, String> constraints);

  /**
   * isZoneIsolationDisabled checks if zone isolation should be disabled. By default, it should be
   * enabled.
   *
   * @return boolean indicating if zone isolation should be disabled
   */
  boolean isZoneIsolationDisabled();

  /**
   * isAuthClientInterceptorEnabled checks if dispatcher should be using authClientInterceptor.
   *
   * @param constraints the set of constraints to be considered
   * @return boolean indicating if the channel should use authClientInterceptor filter, default to
   *     false
   */
  boolean isAuthClientInterceptorEnabled(Map<String, String> constraints);
}
