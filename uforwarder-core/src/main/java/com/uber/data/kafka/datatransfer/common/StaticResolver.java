package com.uber.data.kafka.datatransfer.common;

import com.google.api.core.InternalApi;
import com.google.common.net.HostAndPort;

/** StaticResolver is an implementation of HostResolver that returns a static host:port. */
@InternalApi
public final class StaticResolver implements HostResolver {

  private final String host;
  private final int port;

  public StaticResolver(HostAndPort hostAndPort) {
    this(hostAndPort.getHost(), hostAndPort.getPort());
  }

  /**
   * Create a new StaticResolver.
   *
   * @param host to return.
   * @param port to return.
   */
  public StaticResolver(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * getHostPort always returns the fixed host:port provided at construction time.
   *
   * @return HostAndPort provided at construction.
   */
  @Override
  public HostAndPort getHostPort() throws Exception {
    return HostAndPort.fromParts(host, port);
  }
}
