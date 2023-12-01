package com.uber.data.kafka.consumerproxy.config;

import com.uber.data.kafka.consumerproxy.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import java.util.concurrent.Executor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {SchedulerConfiguration.class})
public class SchedulerConfigurationTest extends FievelTestBase {
  @Autowired private Executor executor;

  @Test
  public void testExecutor() {
    Assert.assertNotNull(executor);
  }
}
