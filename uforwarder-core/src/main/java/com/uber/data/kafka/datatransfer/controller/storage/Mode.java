package com.uber.data.kafka.datatransfer.controller.storage;

import com.google.api.core.InternalApi;

/** Mode determines the backend for the storage */
@InternalApi
public enum Mode {
  // LOCAL store is in an in-memory store.
  LOCAL,
  // ZK is a zookeeper backed store.
  ZK;

  public static String MetricsTag = "store_mode";
}
