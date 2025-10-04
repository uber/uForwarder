package com.uber.data.kafka.datatransfer.common;

import com.google.common.net.HostAndPort;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ControllerClientStaticResolverTest {
  private HostAndPort hostPort;
  private StaticResolver resolver;

  @BeforeEach
  public void setup() {
    hostPort = HostAndPort.fromParts("localhost", 8000);
    resolver = new StaticResolver(hostPort);
  }

  @Test
  public void testGetHostPort() throws Exception {
    Assertions.assertEquals(hostPort, resolver.getHostPort());
  }
}
