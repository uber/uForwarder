package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ThreadRegisterTest {
  private ThreadRegister threadRegister;
  private ThreadMXBean threadMXBean;
  private CpuUsageMeter cpuUsageMeter;
  private TestUtils.TestTicker testTicker;

  @BeforeEach
  public void setUp() {
    threadMXBean = Mockito.mock(ThreadMXBean.class);
    testTicker = new TestUtils.TestTicker();
    Mockito.when(threadMXBean.isThreadCpuTimeEnabled()).thenReturn(true);
    cpuUsageMeter = new CpuUsageMeter(testTicker);
    threadRegister = new ThreadRegister(threadMXBean, cpuUsageMeter);
  }

  @Test
  public void testToThreadFactory() {
    ThreadFactory factory = threadRegister.asThreadFactory();
    Thread thread = factory.newThread(() -> {});
    Set<Thread> registered = new HashSet(threadRegister.getRegistered());
    Assertions.assertTrue(registered.contains(thread));
  }

  @Test
  public void testRegisterThread() {
    Thread t = Mockito.mock(Thread.class);
    Thread ret = threadRegister.register(t);
    Assertions.assertEquals(t, ret);

    Set<Thread> registered = new HashSet(threadRegister.getRegistered());
    Assertions.assertTrue(registered.contains(t));
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
    threadRegister.getUsage(); // Establish baseline
    threadRegister.getUsage(); // Calculate usage
    testTicker.add(Duration.ofSeconds(10));
    double usage = threadRegister.getUsage();

    // Then: Usage should be greater than 0 (1000ns = 1 microsecond)
    Assertions.assertTrue(usage > 0.0, "CPU usage should be greater than 0");
  }

  @Test
  public void testGetUsageWhenCpuTimeDisabled() {
    // Given: CPU time disabled
    Mockito.when(threadMXBean.isThreadCpuTimeEnabled()).thenReturn(false);

    // When: Get usage
    double usage = new ThreadRegister(threadMXBean).getUsage();

    // Then: Should return 0.0 when CPU time is disabled
    Assertions.assertTrue(usage == 0.0);
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
    threadRegister.getUsage();

    // Then: Should not call getThreadCpuTime when CPU time is disabled
    Mockito.verify(threadMXBean, Mockito.never()).getThreadCpuTime(Mockito.anyLong());
  }
}
