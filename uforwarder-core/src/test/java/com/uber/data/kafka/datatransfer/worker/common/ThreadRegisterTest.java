package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ThreadRegisterTest extends FievelTestBase {
  private ThreadRegister threadRegister;
  private ThreadMXBean threadMXBean;
  private TestUtils.TestTicker testTicker;

  @Before
  public void setUp() {
    threadMXBean = Mockito.mock(ThreadMXBean.class);
    testTicker = new TestUtils.TestTicker();
    Mockito.when(threadMXBean.isThreadCpuTimeEnabled()).thenReturn(true);
    threadRegister = new ThreadRegister(threadMXBean, testTicker);
  }

  @Test
  public void testToThreadFactory() {
    ThreadFactory factory = threadRegister.asThreadFactory();
    Thread thread = factory.newThread(() -> {});
    Set<Thread> registered = new HashSet(threadRegister.getRegistered());
    Assert.assertTrue(registered.contains(thread));
  }

  @Test
  public void testRegisterThread() {
    Thread t = Mockito.mock(Thread.class);
    Thread ret = threadRegister.register(t);
    Assert.assertEquals(t, ret);

    Set<Thread> registered = new HashSet(threadRegister.getRegistered());
    Assert.assertTrue(registered.contains(t));
  }

  @Test
  public void testTick() {
    // Given: Mock thread with CPU time enabled
    Thread mockThread = Mockito.mock(Thread.class);
    Mockito.when(mockThread.getId()).thenReturn(1L);
    Mockito.when(threadMXBean.isThreadCpuTimeEnabled()).thenReturn(true);
    Mockito.when(threadMXBean.getThreadCpuTime(1L))
        .thenReturn(1000L, 2000L); // First call returns 1000, second call returns 2000

    // When: Register thread and call tick
    threadRegister.register(mockThread);
    threadRegister.tick(); // First tick - establishes baseline
    threadRegister.tick(); // Second tick - calculates CPU usage

    // Then: Verify that getThreadCpuTime was called for the registered thread
    Mockito.verify(threadMXBean, Mockito.times(2)).getThreadCpuTime(1L);
  }

  @Test
  public void testGetUsage() {
    // Given: Mock thread with CPU time enabled and some CPU usage
    Thread mockThread = Mockito.mock(Thread.class);
    Mockito.when(mockThread.getId()).thenReturn(1L);
    Mockito.when(threadMXBean.getThreadCpuTime(1L))
        .thenReturn(0L, TimeUnit.SECONDS.toNanos(5)); // 20s difference

    // When: Register thread, call tick twice, then get usage
    threadRegister.register(mockThread);
    threadRegister.tick(); // Establish baseline
    threadRegister.tick(); // Calculate usage
    testTicker.add(Duration.ofSeconds(10));
    double usage = threadRegister.getUsage();

    // Then: Usage should be greater than 0 (1000ns = 1 microsecond)
    Assert.assertTrue("CPU usage should be greater than 0", usage > 0.0);
  }

  @Test
  public void testGetUsageWhenCpuTimeDisabled() {
    // Given: CPU time disabled
    Mockito.when(threadMXBean.isThreadCpuTimeEnabled()).thenReturn(false);

    // When: Get usage
    double usage = new ThreadRegister(threadMXBean).getUsage();

    // Then: Should return NaN when CPU time is disabled
    Assert.assertTrue("Usage should be NaN when CPU time is disabled", Double.isNaN(usage));
  }

  @Test
  public void testTickWhenCpuTimeDisabled() {
    // Given: CPU time disabled and a registered thread
    Thread mockThread = Mockito.mock(Thread.class);
    Mockito.when(mockThread.getId()).thenReturn(1L);
    Mockito.when(threadMXBean.isThreadCpuTimeEnabled()).thenReturn(false);

    // When: Register thread and call tick
    threadRegister = new ThreadRegister(threadMXBean);
    threadRegister.register(mockThread);
    threadRegister.tick();

    // Then: Should not call getThreadCpuTime when CPU time is disabled
    Mockito.verify(threadMXBean, Mockito.never()).getThreadCpuTime(Mockito.anyLong());
  }

  @Test
  public void testNOOPCpuMeter() {
    ThreadRegister.NOOP.tick();
    Assert.assertEquals(0.0, ThreadRegister.NOOP.getUsage(), 0.0);
  }
}
