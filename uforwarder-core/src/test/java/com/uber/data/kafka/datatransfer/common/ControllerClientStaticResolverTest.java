package com.uber.data.kafka.datatransfer.common;

import com.google.common.net.HostAndPort;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ControllerClientStaticResolverTest extends FievelTestBase {
  private HostAndPort hostPort;
  private StaticResolver resolver;

  @Before
  public void setup() {
    hostPort = HostAndPort.fromParts("localhost", 8000);
    resolver = new StaticResolver(hostPort);
  }

  @Test
  public void testGetHostPort() throws Exception {
    Assert.assertEquals(hostPort, resolver.getHostPort());
  }
}
