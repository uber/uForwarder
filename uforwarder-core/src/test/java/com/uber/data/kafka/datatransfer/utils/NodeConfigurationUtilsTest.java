package com.uber.data.kafka.datatransfer.utils;

import static com.uber.data.kafka.datatransfer.utils.NodeConfigurationUtils.HOSTNAME_KEY;

import com.uber.fievel.testing.base.FievelTestBase;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class NodeConfigurationUtilsTest extends FievelTestBase {
  @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void testDynamicNode() {
    String hostname = "testhost";
    environmentVariables.set(HOSTNAME_KEY, hostname);

    Assert.assertEquals(hostname, NodeConfigurationUtils.getHost());
  }

  @Test
  public void testStaticNode() throws UnknownHostException {
    String hostName = InetAddress.getLocalHost().getHostName();
    Assert.assertEquals(hostName, NodeConfigurationUtils.getHost());
  }
}
