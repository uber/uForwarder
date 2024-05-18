package com.uber.data.kafka.datatransfer.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.commons.lang3.StringUtils;

/** Helper method for node configuration */
public class NodeConfigurationUtils {
  public static final String HOSTNAME_KEY = "UFORWARDER_HOSTNAME";

  private static final String UNKNOWN_HOST = "unknown-host";

  private NodeConfigurationUtils() {}
  /**
   * Gets host name from system environment UFORWARDER_HOSTNAME first and fall back to
   * InetAddress.getLocalHost().getHostName() if it is mssing
   *
   * @return the host name
   */
  public static String getHost() {
    String hostName = System.getenv(HOSTNAME_KEY);
    if (StringUtils.isEmpty(hostName)) {
      try {
        hostName = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        // TODO: throw exception instead of return UNKNOWN_HOST
        hostName = UNKNOWN_HOST;
      }
    }
    return hostName;
  }
}
