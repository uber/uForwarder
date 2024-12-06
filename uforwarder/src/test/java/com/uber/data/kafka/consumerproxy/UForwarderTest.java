package com.uber.data.kafka.consumerproxy;

import com.uber.data.kafka.datatransfer.controller.DataTransferMaster;
import com.uber.data.kafka.datatransfer.worker.DataTransferWorker;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.env.ConfigurableEnvironment;

public class UForwarderTest extends FievelTestBase {

  @Test
  public void testNew() {
    new UForwarder();
  }

  @Test
  public void testBuildConfigurationEnvironment() {

    ConfigurableEnvironment configurableEnvironment =
        UForwarder.buildConfigurationEnvironment(
            UForwarder.getActiveProfiles(UForwarderAppType.CONTROLLER_APP));
    Assert.assertEquals(2, configurableEnvironment.getActiveProfiles().length);
    // TODO: use assert contains to avoid the string order is changed in logic code
    Assert.assertEquals(
        DataTransferMaster.SPRING_PROFILE, configurableEnvironment.getActiveProfiles()[0]);
    Assert.assertEquals(
        UForwarderAppType.CONTROLLER_APP, configurableEnvironment.getActiveProfiles()[1]);
  }

  @Test
  public void testGetActiveProfiles() {
    String[] activeProfiles = UForwarder.getActiveProfiles("");
    Assert.assertEquals(0, activeProfiles.length);

    activeProfiles = UForwarder.getActiveProfiles(UForwarderAppType.CONTROLLER_APP);
    Assert.assertEquals(2, activeProfiles.length);
    Assert.assertEquals(DataTransferMaster.SPRING_PROFILE, activeProfiles[0]);
    Assert.assertEquals(UForwarderAppType.CONTROLLER_APP, activeProfiles[1]);

    activeProfiles = UForwarder.getActiveProfiles(UForwarderAppType.WORKER_APP);
    Assert.assertEquals(2, activeProfiles.length);
    Assert.assertEquals(DataTransferWorker.SPRING_PROFILE, activeProfiles[0]);
    Assert.assertEquals(UForwarderAppType.WORKER_APP, activeProfiles[1]);

    activeProfiles = UForwarder.getActiveProfiles("uforward-mock");
    Assert.assertEquals(0, activeProfiles.length);
  }

  @Test
  public void testProvideActiveProfileFetcher() {
    String[] activeProfiles = UForwarder.provideActiveProfile(new String[] {});
    Assert.assertEquals(0, activeProfiles.length);

    activeProfiles = UForwarder.provideActiveProfile(new String[] {UForwarderAppType.WORKER_APP});
    Assert.assertEquals(2, activeProfiles.length);
    Assert.assertEquals(DataTransferWorker.SPRING_PROFILE, activeProfiles[0]);
    Assert.assertEquals(UForwarderAppType.WORKER_APP, activeProfiles[1]);
  }

  @Test
  public void testMain() {
    // run with ephemeral port to avoid conflicts
    String[] args = new String[] {UForwarderAppType.WORKER_APP, "--server.port=0"};
    UForwarder.main(args);
  }
}
