package com.uber.data.kafka.datatransfer.common.utils;

/**
 * PodIsolationStatus is used by the controller on how to place jobs based on job pod. Currently
 * there are two statuses: 1. DISABLED means no pod isolation is in place. 2. CANARY_ISOLATION means
 * only jobs in the canary job pod will be isolated in job placement.
 */
public enum PodIsolationStatus {
  DISABLED,
  CANARY_ISOLATION
}
