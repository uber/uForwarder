package com.uber.data.kafka.datatransfer.common;

import com.google.api.core.InternalApi;
import com.google.common.net.HostAndPort;

/**
 * Host Resolver is an interface for resolving host:port.
 *
 * <p>Library users may use the provided StaticResolver or implement their own.
 */
@InternalApi
public interface HostResolver {
  HostAndPort getHostPort() throws Exception;
}
