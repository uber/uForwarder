package com.uber.data.kafka.datatransfer.common;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;

/** This class contains common utilities and defaults for using zk. */
public class ZKUtils {
  /** This is the noop ZK version * */
  public static final int NOOP_VERSION = -1;

  /** This is the default retry policy used for apache curator */
  public static final RetryPolicy DEFAULT_RETRY_POLICY =
      new BoundedExponentialBackoffRetry(1, 100, 5);

  private static final int DEFAULT_SESSION_TIMEOUT_MS =
      Integer.getInteger("curator-default-session-timeout", 60 * 1000);
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS =
      Integer.getInteger("curator-default-connection-timeout", 15 * 1000);

  public static CuratorFramework getCuratorFramework(String zkConnection) {
    // https://issues.apache.org/jira/browse/CURATOR-593
    // Disable EnsembleTracker with zk34CompatibilityMode(true)
    // as it can override connection string without set correct chroot, and cause brain split issue
    // TODO: Fix bug in EnsembleTracker, and disable zk34CompatibilityMode
    return CuratorFrameworkFactory.builder()
        .connectString(zkConnection)
        .sessionTimeoutMs(DEFAULT_SESSION_TIMEOUT_MS)
        .connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS)
        .retryPolicy(ZKUtils.DEFAULT_RETRY_POLICY)
        .zk34CompatibilityMode(true)
        .build();
  }
}
