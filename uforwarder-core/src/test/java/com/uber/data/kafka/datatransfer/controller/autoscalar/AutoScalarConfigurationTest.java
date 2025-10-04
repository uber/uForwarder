package com.uber.data.kafka.datatransfer.controller.autoscalar;

import com.uber.data.kafka.datatransfer.utils.UForwarderSpringJUnit4ClassRunner;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@EnableConfigurationProperties
@RunWith(UForwarderSpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {AutoScalarConfiguration.class})
@TestPropertySource(properties = {"spring.config.location=classpath:/base.yaml"})
public class AutoScalarConfigurationTest extends FievelTestBase {
  @Autowired private AutoScalarConfiguration autoScalarConfigurationFromFile;
  private AutoScalarConfiguration autoScalarConfiguration;

  @Before
  public void setup() {
    autoScalarConfiguration = new AutoScalarConfiguration();
  }

  @Test
  public void testLoadFromFile() {
    autoScalarConfiguration = autoScalarConfigurationFromFile;
    Assert.assertEquals(Duration.ofMinutes(6), autoScalarConfiguration.getUpScaleWindowDuration());
    Assert.assertEquals(
        Duration.ofMinutes(10), autoScalarConfiguration.getDownScaleWindowDuration());
    Assert.assertEquals(
        Duration.ofMinutes(20), autoScalarConfiguration.getHibernateWindowDuration());
    Assert.assertEquals(0.1, autoScalarConfiguration.getUpScalePercentile(), 0.00001);
    Assert.assertEquals(0.08, autoScalarConfiguration.getDownScalePercentile(), 0.00001);
    Assert.assertEquals(1.07, autoScalarConfiguration.getUpScaleMinFactor(), 0.00001);
    Assert.assertEquals(0.06, autoScalarConfiguration.getDownScaleMinFactor(), 0.00001);
    Assert.assertEquals(1.05, autoScalarConfiguration.getUpScaleMaxFactor(), 0.00001);
    Assert.assertEquals(0.04, autoScalarConfiguration.getDownScaleMaxFactor(), 0.00001);
    Assert.assertEquals(Duration.ofSeconds(10), autoScalarConfiguration.getThroughputTTL());
    Assert.assertEquals(Duration.ofMinutes(5), autoScalarConfiguration.getJobStatusTTL());
    Assert.assertTrue(autoScalarConfiguration.isDryRun());
    Assert.assertEquals(100, autoScalarConfiguration.getMessagesPerSecPerWorker());
    Assert.assertEquals(1000, autoScalarConfiguration.getBytesPerSecPerWorker());
    Assert.assertTrue(autoScalarConfiguration.isHibernatingEnabled());
    Assert.assertEquals(5.0, autoScalarConfiguration.getCpuUsagePerWorker(), 0.00001);
    Assert.assertEquals(ScaleConverterMode.CPU, autoScalarConfiguration.getScaleConverterMode());
    Assert.assertEquals(
        ScaleConverterMode.THROUGHPUT, autoScalarConfiguration.getShadowScaleConverterMode().get());
    Assert.assertEquals(
        Duration.ofMinutes(10), autoScalarConfiguration.getReactiveScaleWindowDuration());
    Assert.assertTrue(autoScalarConfiguration.isReactiveScaleWindowEnabled());
  }

  @Test
  public void testThroughputExpirationMs() {
    autoScalarConfiguration.setThroughputTTL(Duration.ofMillis(100));
    Duration result = autoScalarConfiguration.getThroughputTTL();
    Assert.assertEquals(Duration.ofMillis(100), result);
  }

  @Test
  public void testJobStatusTTL() {
    autoScalarConfiguration.setJobStatusTTL(Duration.ofMillis(100));
    Duration result = autoScalarConfiguration.getJobStatusTTL();
    Assert.assertEquals(Duration.ofMillis(100), result);
  }

  @Test
  public void testUpScaleWindowDuration() {
    autoScalarConfiguration.setUpScaleWindowDuration(Duration.ofSeconds(101));
    Duration result = autoScalarConfiguration.getUpScaleWindowDuration();
    Assert.assertEquals(Duration.ofSeconds(101), result);
  }

  @Test
  public void testDownScaleWindowDuration() {
    autoScalarConfiguration.setDownScaleWindowDuration(Duration.ofSeconds(101));
    Duration result = autoScalarConfiguration.getDownScaleWindowDuration();
    Assert.assertEquals(Duration.ofSeconds(101), result);
  }

  @Test
  public void testHibernateWindowDuration() {
    autoScalarConfiguration.setHibernateWindowDuration(Duration.ofSeconds(101));
    Duration result = autoScalarConfiguration.getHibernateWindowDuration();
    Assert.assertEquals(Duration.ofSeconds(101), result);
  }

  @Test
  public void testUpScalePercentile() {
    autoScalarConfiguration.setUpScalePercentile(0.12);
    double result = autoScalarConfiguration.getUpScalePercentile();
    Assert.assertEquals(0.12, result, 0.00001);
  }

  @Test
  public void testDownScalePercentile() {
    autoScalarConfiguration.setDownScalePercentile(0.22);
    double result = autoScalarConfiguration.getDownScalePercentile();
    Assert.assertEquals(0.22, result, 0.00001);
  }

  @Test
  public void testUpScaleMinFactor() {
    autoScalarConfiguration.setUpScaleMinFactor(1.12);
    double result = autoScalarConfiguration.getUpScaleMinFactor();
    Assert.assertEquals(1.12, result, 0.00001);
  }

  @Test
  public void testDownScaleMinFactor() {
    autoScalarConfiguration.setDownScaleMinFactor(0.22);
    double result = autoScalarConfiguration.getDownScaleMinFactor();
    Assert.assertEquals(0.22, result, 0.00001);
  }

  @Test
  public void testUpScaleMaxFactor() {
    autoScalarConfiguration.setUpScaleMaxFactor(1.12);
    double result = autoScalarConfiguration.getUpScaleMaxFactor();
    Assert.assertEquals(1.12, result, 0.00001);
  }

  @Test
  public void testDownScaleMaxFactor() {
    autoScalarConfiguration.setDownScaleMaxFactor(0.22);
    double result = autoScalarConfiguration.getDownScaleMaxFactor();
    Assert.assertEquals(0.22, result, 0.00001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDownScaleMaxFactorFailed() {
    autoScalarConfiguration.setDownScaleMaxFactor(1.1);
  }

  @Test
  public void testDryRun() {
    Assert.assertTrue(autoScalarConfiguration.isDryRun());
    autoScalarConfiguration.setDryRun(false);
    Assert.assertFalse(autoScalarConfiguration.isDryRun());
  }

  @Test
  public void testMessageRatePerWorker() {
    autoScalarConfiguration.setMessagesPerSecPerWorker(200);
    Assert.assertEquals(200, autoScalarConfiguration.getMessagesPerSecPerWorker());
  }

  @Test
  public void testByteRatePerWorker() {
    autoScalarConfiguration.setBytesPerSecPerWorker(2000);
    Assert.assertEquals(2000, autoScalarConfiguration.getBytesPerSecPerWorker());
  }

  @Test
  public void testHibernatingEnabled() {
    Assert.assertFalse(autoScalarConfiguration.isHibernatingEnabled());
    autoScalarConfiguration.setHibernatingEnabled(true);
    Assert.assertTrue(autoScalarConfiguration.isHibernatingEnabled());
  }

  @Test
  public void testReactiveScaleWindowEnabled() {
    Assert.assertFalse(autoScalarConfiguration.isReactiveScaleWindowEnabled());
    autoScalarConfiguration.setReactiveScaleWindowEnabled(true);
    Assert.assertTrue(autoScalarConfiguration.isReactiveScaleWindowEnabled());
  }
}
