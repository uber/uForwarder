package com.uber.data.kafka.datatransfer.worker.common;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ThreadRegisterTest extends FievelTestBase {
  private ThreadRegister threadRegister;

  @Before
  public void setUp() {
    threadRegister = new ThreadRegister();
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
    // TODO: implement test
    threadRegister.tick();
  }

  @Test
  public void testGetUsage() {
    // TODO: implement test
    Assert.assertEquals(0.0, threadRegister.getUsage(), 0.000001);
  }
}
