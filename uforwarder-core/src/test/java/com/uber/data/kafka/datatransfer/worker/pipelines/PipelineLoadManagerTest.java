package com.uber.data.kafka.datatransfer.worker.pipelines;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.sun.management.OperatingSystemMXBean;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.data.kafka.datatransfer.worker.common.ThreadRegister;
import com.uber.fievel.testing.base.FievelTestBase;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class PipelineLoadManagerTest extends FievelTestBase {

  private CoreInfra coreInfra;
  private PipelineLoadManager pipelineLoadManager;
  private TestUtils.TestTicker ticker;
  private ThreadMXBean threadMXBean;
  private OperatingSystemMXBean operatingSystemMXBean;

  @Before
  public void setUp() {
    coreInfra = Mockito.mock(CoreInfra.class);
    ticker = new TestUtils.TestTicker();
    threadMXBean = Mockito.mock(ThreadMXBean.class);
    operatingSystemMXBean = Mockito.mock(OperatingSystemMXBean.class);

    when(coreInfra.getThreadMXBean()).thenReturn(threadMXBean);
    when(coreInfra.getOperatingSystemMXBean()).thenReturn(operatingSystemMXBean);
    when(threadMXBean.isThreadCpuTimeEnabled()).thenReturn(true);

    pipelineLoadManager = new PipelineLoadManager(coreInfra, ticker);
  }

  @Test
  public void testCreateTracker() {
    // Test creating a new tracker
    String pipelineId = "test-pipeline-1";
    PipelineLoadManager.LoadTracker tracker = pipelineLoadManager.createTracker(pipelineId);

    assertNotNull("Tracker should not be null", tracker);
    assertEquals("Pipeline ID should match", pipelineId, tracker.pipelineId());
    assertNotNull("Thread register should be created", tracker.getThreadRegister());

    // Test creating the same tracker again (should return the same instance)
    PipelineLoadManager.LoadTracker sameTracker = pipelineLoadManager.createTracker(pipelineId);
    assertSame("Should return the same tracker instance", tracker, sameTracker);

    // Test creating a different tracker
    String pipelineId2 = "test-pipeline-2";
    PipelineLoadManager.LoadTracker tracker2 = pipelineLoadManager.createTracker(pipelineId2);
    assertNotNull("Second tracker should not be null", tracker2);
    assertEquals("Second pipeline ID should match", pipelineId2, tracker2.pipelineId());
  }

  @Test
  public void testGetThreadRegister() {
    String pipelineId = "test-pipeline";
    PipelineLoadManager.LoadTracker tracker = pipelineLoadManager.createTracker(pipelineId);

    ThreadRegister threadRegister = tracker.getThreadRegister();
    assertNotNull("Thread register should not be null", threadRegister);
  }

  @Test
  public void testGetLoad() {
    String pipelineId = "test-pipeline";
    PipelineLoadManager.LoadTracker tracker = pipelineLoadManager.createTracker(pipelineId);

    // Mock system load average
    when(operatingSystemMXBean.getSystemLoadAverage()).thenReturn(2.5);

    // Test initial load (should be 0 since no CPU time has been recorded)
    PipelineLoadTracker.PipelineLoad load = tracker.getLoad();
    assertNotNull("Load should not be null", load);
    assertEquals("Core thread usage should be 0 initially", 0.0, load.getCoreCpuUsage(), 0.001);
    assertEquals("Shared system usage should be 0 initially", 0.0, load.getCpuUsage(), 0.001);

    // Test with non-zero system load
    when(operatingSystemMXBean.getSystemLoadAverage()).thenReturn(1.5);
    PipelineLoadTracker.PipelineLoad load2 = tracker.getLoad();
    assertEquals("Core thread usage should still be 0", 0.0, load2.getCoreCpuUsage(), 0.001);
    assertEquals(
        "Shared system usage should be 0 when share is 0", 0.0, load2.getCpuUsage(), 0.001);
  }

  @Test
  public void testGetLoadWithMultipleTrackers() {
    // Create multiple trackers
    PipelineLoadManager.LoadTracker tracker1 = pipelineLoadManager.createTracker("pipeline-1");
    PipelineLoadManager.LoadTracker tracker2 = pipelineLoadManager.createTracker("pipeline-2");

    when(operatingSystemMXBean.getSystemLoadAverage()).thenReturn(3.0);

    // Both should return zero load initially
    PipelineLoadTracker.PipelineLoad load1 = tracker1.getLoad();
    PipelineLoadTracker.PipelineLoad load2 = tracker2.getLoad();

    assertEquals("First tracker core usage should be 0", 0.0, load1.getCoreCpuUsage(), 0.001);
    assertEquals("First tracker shared usage should be 0", 0.0, load1.getCpuUsage(), 0.001);
    assertEquals("Second tracker core usage should be 0", 0.0, load2.getCoreCpuUsage(), 0.001);
    assertEquals("Second tracker shared usage should be 0", 0.0, load2.getCpuUsage(), 0.001);
  }

  @Test
  public void testGetSharedSystemUsage() {
    String pipelineId = "test-pipeline";
    // Mock system load average
    try (MockedConstruction<ThreadRegister> mockedConstruction =
        Mockito.mockConstruction(ThreadRegister.class)) {
      // Code that creates new instances of MyClass will now return mocks
      PipelineLoadManager.LoadTracker tracker = pipelineLoadManager.createTracker(pipelineId);
      // You can then configure the behavior of these mock instances
      when(tracker.getThreadRegister().getUsage()).thenReturn(0.5);
      for (int i = 0; i < 20; i++) {
        ticker.add(Duration.ofSeconds(1));
        when(operatingSystemMXBean.getProcessCpuTime()).thenReturn(ticker.read());
        tracker.getLoad();
      }
      Assert.assertEquals(1.0, tracker.getLoad().getCpuUsage(), 0.0000001);
    }
  }

  @Test
  public void testGetSharedSystemUsageWithMultiplePipeline() {
    String pipelineId1 = "test-pipeline-1";
    String pipelineId2 = "test-pipeline-2";
    // Mock system load average
    try (MockedConstruction<ThreadRegister> mockedConstruction =
        Mockito.mockConstruction(ThreadRegister.class)) {
      PipelineLoadManager.LoadTracker tracker1 = pipelineLoadManager.createTracker(pipelineId1);
      PipelineLoadManager.LoadTracker tracker2 = pipelineLoadManager.createTracker(pipelineId2);
      // You can then configure the behavior of these mock instances
      when(tracker1.getThreadRegister().getUsage()).thenReturn(0.3);
      when(tracker2.getThreadRegister().getUsage()).thenReturn(0.2);
      for (int i = 0; i < 20; i++) {
        ticker.add(Duration.ofSeconds(1));
        when(operatingSystemMXBean.getProcessCpuTime()).thenReturn(ticker.read());
        tracker1.getLoad();
      }
      Assert.assertEquals(0.6, tracker1.getLoad().getCpuUsage(), 0.0000001);
      Assert.assertEquals(0.4, tracker2.getLoad().getCpuUsage(), 0.0000001);
    }
  }

  @Test
  public void testClose() {
    String pipelineId = "test-pipeline";
    PipelineLoadManager.LoadTracker tracker = pipelineLoadManager.createTracker(pipelineId);

    // Test first close (should succeed)
    boolean firstClose = tracker.close();
    assertTrue("First close should return true", firstClose);

    // Test second close (should fail)
    boolean secondClose = tracker.close();
    assertFalse("Second close should return false", secondClose);

    // Verify tracker is removed from the manager
    PipelineLoadManager.LoadTracker newTracker = pipelineLoadManager.createTracker(pipelineId);
    assertNotNull("New tracker should be created after close", newTracker);
    assertNotSame("New tracker should be different instance", tracker, newTracker);
  }
}
