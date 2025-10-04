package com.uber.data.kafka.datatransfer.common;

import com.uber.data.kafka.datatransfer.worker.common.CpuUsageMeter;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CpuUsageMeterTest extends FievelTestBase {
  private CpuUsageMeter cpuUsageMeter;
  private TestUtils.TestTicker testTicker;

  @Before
  public void setUp() {
    testTicker = new TestUtils.TestTicker();
    cpuUsageMeter = new CpuUsageMeter(testTicker);
  }

  @Test
  public void testMarkAndGet() {
    Duration d = Duration.ofSeconds(1);
    for (int i = 0; i < 10; ++i) {
      testTicker.add(d);
      cpuUsageMeter.mark(d.toNanos() / 2);
    }
    double usage = cpuUsageMeter.getUsage();
    Assert.assertEquals(0.5, usage, 0.000000001);
  }

  @Test
  public void testGetWithoutMark() {
    double usage = cpuUsageMeter.getUsage();
    Assert.assertEquals(0.0, usage, 0.000000001);
  }
}
