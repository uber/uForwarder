package com.uber.data.kafka.datatransfer.utils;

import static com.uber.data.kafka.datatransfer.utils.NodeConfigurationUtils.HOSTNAME_KEY;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

@ExtendWith(SystemStubsExtension.class)
public class NodeConfigurationUtilsTest {
  @SystemStub public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void testDynamicNode() {
    String hostname = "testhost";
    environmentVariables.set(HOSTNAME_KEY, hostname);

    Assertions.assertEquals(hostname, NodeConfigurationUtils.getHost());
  }

  @Test
  public void testStaticNode() throws UnknownHostException {
    String hostName = InetAddress.getLocalHost().getHostName();
    Assertions.assertEquals(hostName, NodeConfigurationUtils.getHost());
  }
}
