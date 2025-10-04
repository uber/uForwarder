package com.uber.data.kafka.datatransfer.worker.pipelines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.sun.management.OperatingSystemMXBean;
import com.uber.data.kafka.datatransfer.common.CoreInfra;
import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.data.kafka.datatransfer.worker.common.ThreadRegister;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class PipelineLoadManagerTest {

  private CoreInfra coreInfra;
  private PipelineLoadManager pipelineLoadManager;
  private TestUtils.TestTicker ticker;
  private ThreadMXBean threadMXBean;
  private OperatingSystemMXBean operatingSystemMXBean;

  @BeforeEach
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

    assertNotNull(tracker, "Tracker should not be null");
    assertEquals(pipelineId, tracker.pipelineId(), "Pipeline ID should match");
    assertNotNull(tracker.getThreadRegister(), "Thread register should be created");

    // Test creating the same tracker again (should return the same instance)
    PipelineLoadManager.LoadTracker sameTracker = pipelineLoadManager.createTracker(pipelineId);
    assertSame(tracker, sameTracker, "Should return the same tracker instance");

    // Test creating a different tracker
    String pipelineId2 = "test-pipeline-2";
    PipelineLoadManager.LoadTracker tracker2 = pipelineLoadManager.createTracker(pipelineId2);
    assertNotNull(tracker2, "Second tracker should not be null");
    assertEquals(pipelineId2, tracker2.pipelineId(), "Second pipeline ID should match");
  }

  @Test
  public void testGetThreadRegister() {
    String pipelineId = "test-pipeline";
    PipelineLoadManager.LoadTracker tracker = pipelineLoadManager.createTracker(pipelineId);

    ThreadRegister threadRegister = tracker.getThreadRegister();
    assertNotNull(threadRegister, "Thread register should not be null");
  }

  @Test
  public void testGetLoad() {
    String pipelineId = "test-pipeline";
    PipelineLoadManager.LoadTracker tracker = pipelineLoadManager.createTracker(pipelineId);

    // Mock system load average
    when(operatingSystemMXBean.getSystemLoadAverage()).thenReturn(2.5);

    // Test initial load (should be 0 since no CPU time has been recorded)
    PipelineLoadTracker.PipelineLoad load = tracker.getLoad();
    assertNotNull(load, "Load should not be null");
    assertEquals(0.0, load.getCoreCpuUsage(), 0.001, "Core thread usage should be 0 initially");
    assertEquals(0.0, load.getCpuUsage(), 0.001, "Shared system usage should be 0 initially");

    // Test with non-zero system load
    when(operatingSystemMXBean.getSystemLoadAverage()).thenReturn(1.5);
    PipelineLoadTracker.PipelineLoad load2 = tracker.getLoad();
    assertEquals(0.0, load2.getCoreCpuUsage(), 0.001, "Core thread usage should still be 0");
    assertEquals(
        0.0, load2.getCpuUsage(), 0.001, "Shared system usage should be 0 when share is 0");
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

    assertEquals(0.0, load1.getCoreCpuUsage(), 0.001, "First tracker core usage should be 0");
    assertEquals(0.0, load1.getCpuUsage(), 0.001, "First tracker shared usage should be 0");
    assertEquals(0.0, load2.getCoreCpuUsage(), 0.001, "Second tracker core usage should be 0");
    assertEquals(0.0, load2.getCpuUsage(), 0.001, "Second tracker shared usage should be 0");
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
      Assertions.assertEquals(1.0, tracker.getLoad().getCpuUsage(), 0.0000001);
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
      Assertions.assertEquals(0.6, tracker1.getLoad().getCpuUsage(), 0.0000001);
      Assertions.assertEquals(0.4, tracker2.getLoad().getCpuUsage(), 0.0000001);
    }
  }

  @Test
  public void testClose() {
    String pipelineId = "test-pipeline";
    PipelineLoadManager.LoadTracker tracker = pipelineLoadManager.createTracker(pipelineId);

    // Test first close (should succeed)
    boolean firstClose = tracker.close();
    assertTrue(firstClose, "First close should return true");

    // Test second close (should fail)
    boolean secondClose = tracker.close();
    assertFalse(secondClose, "Second close should return false");

    // Verify tracker is removed from the manager
    PipelineLoadManager.LoadTracker newTracker = pipelineLoadManager.createTracker(pipelineId);
    assertNotNull(newTracker, "New tracker should be created after close");
    assertNotSame(tracker, newTracker, "New tracker should be different instance");
  }
}
