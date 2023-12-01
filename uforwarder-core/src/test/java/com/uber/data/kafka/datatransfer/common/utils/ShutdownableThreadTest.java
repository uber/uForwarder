package com.uber.data.kafka.datatransfer.common.utils;

import static org.junit.Assert.assertEquals;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Exit;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Test;

// This class is mimic from
// https://github.com/apache/kafka/blob/3.6/core/src/test/scala/unit/kafka/utils/ShutdownableThreadTest.scala
public class ShutdownableThreadTest extends FievelTestBase {

  @After
  public void tearDown() {
    Exit.resetExitProcedure();
  }

  @Test
  public void testShutdownWhenCalledAfterThreadStart() throws InterruptedException {
    AtomicReference<Integer> statusCodeRef = new AtomicReference<>();
    Exit.setExitProcedure(new MockExitProcedure(statusCodeRef));
    CountDownLatch latch = new CountDownLatch(1);
    ShutdownableThread thread =
        new ShutdownableThread("shutdownable-thread-test") {
          @Override
          public void doWork() {
            latch.countDown();
            throw new FatalExitError();
          }
        };
    thread.start();
    // "doWork was not invoked"
    Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> latch.getCount() == 0);

    thread.shutdown();
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> statusCodeRef.get() != null);
    assertEquals(1, statusCodeRef.get().intValue());
  }

  public static class MockExitProcedure implements Exit.Procedure {
    private boolean hasExited = false;
    private final AtomicReference<Integer> statusCodeRef;

    public MockExitProcedure(AtomicReference<Integer> statusCodeRef) {
      this.statusCodeRef = statusCodeRef;
    }

    @Override
    public void execute(int statusCode, String message) {
      if (!this.hasExited) {
        this.hasExited = true;
        this.statusCodeRef.set(statusCode);
        // Sleep until interrupted to emulate the fact that `System.exit()` never returns
        AtomicInteger counter = new AtomicInteger(0);
        Awaitility.await()
            .atMost(Long.MAX_VALUE, TimeUnit.MILLISECONDS)
            .until(() -> counter.get() == 1);
        throw new AssertionError("Should never reach here");
      }
    }
  }
}
