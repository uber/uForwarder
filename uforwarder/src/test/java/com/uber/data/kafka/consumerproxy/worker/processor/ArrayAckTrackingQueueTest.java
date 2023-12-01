package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class ArrayAckTrackingQueueTest extends FievelTestBase {
  private static final int SIZE = 3;
  private ArrayAckTrackingQueue queue;
  private Scope scope;
  private Gauge gauge;
  private Job job;

  @Before
  public void setUp() {
    scope = Mockito.mock(Scope.class);
    Timer timer = Mockito.mock(Timer.class);
    gauge = Mockito.mock(Gauge.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    job = Job.newBuilder().build();
    queue = new ArrayAckTrackingQueue(job, SIZE, scope);
  }

  @Test
  public void testGetTopicPartition() {
    Assert.assertEquals(job.getKafkaConsumerTask().getTopic(), queue.getTopicPartition().topic());
    Assert.assertEquals(
        job.getKafkaConsumerTask().getPartition(), queue.getTopicPartition().partition());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeCapacity1() {
    new ArrayAckTrackingQueue(job, 0, scope);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeCapacity2() {
    new ArrayAckTrackingQueue(job, -5, scope);
  }

  @Test
  public void testInitialValue() {
    assertValues(0, -1, -1, -1, 0, 0, 0, 0);
    Assert.assertEquals(SIZE, queue.items.length);
    Assert.assertEquals(SIZE, queue.getState().capacity());
    for (ArrayAckTrackingQueue.OffsetStatus status : queue.items) {
      Assert.assertEquals(ArrayAckTrackingQueue.AckStatus.UNSET, status.ackStatus);
    }
  }

  @Test
  public void testReceiveNormally() throws InterruptedException {
    queue.receive(99, "a");
    assertValues(0, 99, 99, 99, 1, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
    queue.receive(100, "b");
    assertKeyStatus("b", 1, 0, 0);
    assertValues(0, 100, 99, 99, 2, 0, 0, 0);
    queue.receive(101, "b");
    assertValues(0, 101, 99, 99, 3, 0, 0, 0);
    assertKeyStatus("b", 2, 0, 0);
  }

  @Test
  public void testReceiveReset() throws InterruptedException {
    AtomicInteger count = new AtomicInteger(0);

    queue.addReactor(() -> count.incrementAndGet());
    queue.receive(99, "a");
    assertValues(0, 99, 99, 99, 1, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
    queue.receive(101, "a");
    assertValues(0, 101, 101, 101, 1, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
    Assert.assertEquals(1, count.get());
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testReceiveBlockedAndAck() throws InterruptedException {
    queue.receive(99, "b");
    queue.receive(100, "a");
    queue.receive(101, "a");
    new Thread(
            () -> {
              try {
                Thread.sleep(5);
                queue.ack(100);
              } catch (Exception ignored) {
              }
            })
        .start();
    queue.receive(102, "a");
    assertKeyStatus("a", 3, 0, 0);
    assertKeyStatus("b", 0, 0, 0);
    Assert.assertFalse(queue.notInUse);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testReceiveBlockedAndNotInUse() throws InterruptedException {
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    new Thread(
            () -> {
              try {
                Thread.sleep(5);
                queue.markAsNotInUse();
              } catch (Exception ignored) {
              }
            })
        .start();
    queue.receive(102);
    Assert.assertTrue(queue.notInUse);
  }

  @Test
  public void testAck() throws InterruptedException {
    // before receive
    Assert.assertEquals(AckTrackingQueue.CANNOT_ACK, queue.ack(99));
    assertValues(0, -1, -1, -1, 0, 0, 0, 0);
    // ack a too large offset
    queue.receive(99, "a");
    Assert.assertEquals(AckTrackingQueue.CANNOT_ACK, queue.ack(200));
    assertValues(0, 99, 99, 99, 1, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
    // ack a too small offset
    Assert.assertEquals(AckTrackingQueue.CANNOT_ACK, queue.ack(99));
    assertValues(0, 99, 99, 99, 1, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
    // ack normally
    Assert.assertEquals(100, queue.ack(100));
    assertValues(1, 99, 100, -1, 0, 0, 0, 0);
    assertKeyStatus("a", 0, 0, 0);
  }

  @Test
  public void testInOrderAck() throws InterruptedException {
    queue.receive(99, "a");
    queue.receive(100);
    queue.receive(101, "a");
    Assert.assertEquals(100, queue.ack(100));
    assertValues(1, 101, 100, 100, 2, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
    Assert.assertEquals(101, queue.ack(101));
    assertValues(2, 101, 101, 101, 1, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
    Assert.assertEquals(102, queue.ack(102));
    assertValues(0, 101, 102, -1, 0, 0, 0, 0);
    assertKeyStatus("a", 0, 0, 0);
  }

  @Test
  public void testOutOfOrderAck() throws InterruptedException {
    queue.receive(99, "a");
    queue.receive(100);
    queue.receive(101, "a");
    Assert.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(0, 101, 99, 99, 3, 1, 0, 1);
    assertKeyStatus("a", 2, 0, 0);
    Assert.assertEquals(AckTrackingQueue.DUPLICATED_ACK, queue.ack(101));
    assertValues(0, 101, 99, 99, 3, 1, 0, 1);
    assertKeyStatus("a", 2, 0, 0);
    Assert.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(102));
    assertValues(0, 101, 99, 99, 3, 2, 0, 2);
    assertKeyStatus("a", 2, 1, 0);
    Assert.assertEquals(102, queue.ack(100));
    assertValues(0, 101, 102, -1, 0, 0, 0, 0);
    assertKeyStatus("a", 0, 0, 0);
  }

  @Test
  public void testOutOfOrderAckWithMetrics() throws InterruptedException {
    Gauge uncommitedGauage = Mockito.mock(Gauge.class);
    Mockito.when(scope.gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED))
        .thenReturn(uncommitedGauage);
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assert.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(0, 101, 99, 99, 3, 1, 0, 1);
    Mockito.verify(scope, Mockito.times(1))
        .gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED);
    Mockito.verify(uncommitedGauage, Mockito.times(1)).update(3);

    // commit first message in the queue,
    Assert.assertEquals(101, queue.ack(100));
    assertValues(2, 101, 101, 101, 1, 0, 0, 0);
    Mockito.verify(scope, Mockito.times(2))
        .gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED);
    Mockito.verify(uncommitedGauage, Mockito.times(1)).update(1);

    queue.publishMetrics();
    Mockito.verify(scope, Mockito.times(3))
        .gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED);
    Mockito.verify(uncommitedGauage, Mockito.times(2)).update(1);
  }

  // this should never happen, but in case something is wrong, we test this case
  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testAckBlockedAndAck() throws InterruptedException {
    queue.receive(99);
    // we cheat to mark 102 as received
    queue.highestReceivedOffset = 102;
    new Thread(
            () -> {
              try {
                Thread.sleep(5);
                queue.ack(100);
              } catch (Exception ignored) {
              }
            })
        .start();
    Assert.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(103));
    queue.updateState();
    assertValues(1, 102, 100, 100, 3, 1, 0, 2);
    Assert.assertFalse(queue.notInUse);
  }

  // this should never happen, but in case something is wrong, we test this case
  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testAckBlockedAndNotInUse() throws InterruptedException {
    queue.receive(99);
    // we cheat to mark 102 as received
    queue.highestReceivedOffset = 102;
    new Thread(
            () -> {
              try {
                Thread.sleep(5);
                queue.markAsNotInUse();
              } catch (Exception ignored) {
              }
            })
        .start();
    Assert.assertEquals(AckTrackingQueue.CANNOT_ACK, queue.ack(103));
    queue.updateState();
    assertValues(0, 102, 99, 99, 4, 0, 0, 0);
    Assert.assertTrue(queue.notInUse);
  }

  @Test
  public void testNack() throws InterruptedException {
    // before receive
    Assert.assertFalse(queue.nack(99));
    assertValues(0, -1, -1, -1, 0, 0, 0, 0);
    // nack a too large offset
    queue.receive(99, "a");
    Assert.assertFalse(queue.nack(200));
    assertValues(0, 99, 99, 99, 1, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
    // nack a too small offset
    Assert.assertFalse(queue.nack(99));
    assertValues(0, 99, 99, 99, 1, 0, 0, 0);
    // nack normally
    Assert.assertTrue(queue.nack(100));
    assertValues(0, 99, 99, 99, 1, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
    // nack a nacked offset
    Assert.assertFalse(queue.nack(100));
    assertValues(0, 99, 99, 99, 1, 0, 0, 0);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testNackBlockedAndAck() throws InterruptedException {
    queue.receive(99);
    // we cheat to mark 102 as received
    queue.highestReceivedOffset = 102;
    new Thread(
            () -> {
              try {
                Thread.sleep(5);
                queue.ack(100);
              } catch (Exception ignored) {
              }
            })
        .start();
    Assert.assertTrue(queue.nack(103));
    assertValues(1, 102, 100, 100, 3, 0, 0, 0);
    Assert.assertFalse(queue.notInUse);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testNackBlockedAndNotInUse() throws InterruptedException {
    queue.receive(99);
    // we cheat to mark 102 as received
    queue.highestReceivedOffset = 102;
    new Thread(
            () -> {
              try {
                Thread.sleep(5);
                queue.markAsNotInUse();
              } catch (Exception ignored) {
              }
            })
        .start();
    Assert.assertFalse(queue.nack(103));
    queue.updateState();
    assertValues(0, 102, 99, 99, 4, 0, 0, 0);
    Assert.assertTrue(queue.notInUse);
  }

  @Test
  public void testNackAfterAck() throws InterruptedException {
    // set up
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assert.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(0, 101, 99, 99, 3, 1, 0, 1);
    Assert.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(102));
    assertValues(0, 101, 99, 99, 3, 2, 0, 2);
    // test
    Assert.assertFalse(queue.nack(101));
    Assert.assertFalse(queue.nack(102));
  }

  @Test
  public void testAckAfterNack() throws InterruptedException {
    // set up
    queue.receive(99, "a");
    queue.receive(100);
    queue.receive(101, "a");
    Assert.assertTrue(queue.nack(100));
    Assert.assertTrue(queue.nack(101));
    Assert.assertTrue(queue.nack(102));
    assertValues(0, 101, 99, 99, 3, 0, 0, 0);
    // test: reuse the code
    testOutOfOrderAck();
  }

  @Test
  public void testAckAfterCancel() throws InterruptedException {
    // set up
    queue.receive(99, "a");
    queue.receive(100, "b");
    queue.receive(101, "a");
    Assert.assertTrue(queue.cancel(100));
    assertValues(0, 101, 99, 100, 3, 0, 1, 0);
    assertKeyStatus("a", 2, 0, 1);
    Assert.assertEquals(100, queue.ack(100));
    assertValues(1, 101, 100, 100, 2, 0, 0, 0);
    assertKeyStatus("a", 1, 0, 0);
  }

  @Test
  public void testAckNextAfterCancel() throws InterruptedException {
    // set up
    queue.receive(99, "a");
    queue.receive(100, "a");
    queue.receive(101, "b");
    Assert.assertTrue(queue.cancel(100));
    assertValues(0, 101, 99, 100, 3, 0, 1, 0);
    assertKeyStatus("a", 2, 0, 1);
    Assert.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(0, 101, 99, 101, 3, 1, 1, 1);
    assertKeyStatus("a", 2, 1, 1);
  }

  @Test
  public void testCancelAfterAck() throws InterruptedException {
    // set up
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assert.assertEquals(100, queue.ack(100));
    assertValues(1, 101, 100, 100, 2, 0, 0, 0);
    Assert.assertFalse(queue.cancel(100));
    assertValues(1, 101, 100, 100, 2, 0, 0, 0);
  }

  @Test
  public void testCancelMoveOffset() throws InterruptedException {
    // set up
    queue.receive(99, "a");
    queue.receive(100, "a");
    queue.receive(101, "b");
    Assert.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(0, 101, 99, 99, 3, 1, 0, 1);
    assertKeyStatus("a", 2, 1, 0);
    Assert.assertTrue(queue.cancel(100));
    assertValues(0, 101, 99, 101, 3, 1, 1, 1);
    assertKeyStatus("a", 2, 1, 1);
  }

  @Test
  public void testCancelAll() throws InterruptedException {
    // set up
    queue.receive(99, "a");
    queue.receive(100, "a");
    queue.receive(101, "b");
    queue.cancel(100);
    queue.cancel(101);
    queue.cancel(102);
    assertValues(0, 101, 99, -1, 3, 0, 3, 0);
    assertKeyStatus("a", 2, 0, 2);
    assertKeyStatus("b", 1, 0, 1);
  }

  @Test
  public void testCancelAfterNack() throws InterruptedException {
    // set up
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assert.assertEquals(true, queue.nack(100));
    assertValues(0, 101, 99, 99, 3, 0, 0, 0);
    Assert.assertTrue(queue.cancel(100));
    assertValues(0, 101, 99, 100, 3, 0, 1, 0);
  }

  @Test
  public void testNackAfterCancel() throws InterruptedException {
    // set up
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assert.assertTrue(queue.cancel(100));
    assertValues(0, 101, 99, 100, 3, 0, 1, 0);
    Assert.assertEquals(false, queue.nack(100));
    assertValues(0, 101, 99, 100, 3, 0, 1, 0);
  }

  @Test
  public void testPublishMetrics() {
    queue.publishMetrics();
    Mockito.verify(scope, Mockito.times(1)).gauge("tracking-queue.blocking.load-factor");
    Mockito.verify(scope, Mockito.times(1)).gauge("tracking-queue.blocking.load-factor-exclusive");
    Mockito.verify(scope, Mockito.times(1)).gauge("tracking-queue.blocking.acked");
    Mockito.verify(scope, Mockito.times(1)).gauge("tracking-queue.blocking.cancelled");
  }

  private void assertValues(
      int headIndex,
      long highestReceivedOffset,
      long offsetMappingHeadIndex,
      long lowestCancelableOffset,
      int size,
      int ackedCount,
      int canceled,
      int width) {
    Assert.assertEquals(headIndex, queue.headIndex);
    Assert.assertEquals(highestReceivedOffset, queue.highestReceivedOffset);
    Assert.assertEquals(offsetMappingHeadIndex, queue.offsetMappingHeadIndex);
    Assert.assertEquals(lowestCancelableOffset, queue.getState().lowestCancelableOffset());
    Assert.assertEquals(size == 0 ? -1 : offsetMappingHeadIndex, queue.getState().headOffset());
    Assert.assertEquals(size, queue.getState().stats().size());
    Assert.assertEquals(ackedCount, queue.getState().stats().acked());
    Assert.assertEquals(canceled, queue.getState().stats().canceled());
    Assert.assertEquals(width, width(queue.getState()));
    Assert.assertEquals(size, length(queue.getState()));
  }

  private void assertKeyStatus(String key, int size, int acked, int canceled) {
    AckTrackingQueue.Stats stats = queue.getState().keyStats().get(key);
    Assert.assertEquals(size, stats.size());
    Assert.assertEquals(acked, stats.acked());
    Assert.assertEquals(canceled, stats.canceled());
  }

  // TODO: 1.remove width and length. 2. validate offsets instead of width nor length
  private int width(AckTrackingQueue.State state) {
    if (state.highestAckedOffset() == -1 || state.headOffset() == -1) {
      return 0;
    }

    return (int) Math.max(0L, state.highestAckedOffset() - state.headOffset() - 1);
  }

  private int length(AckTrackingQueue.State state) {
    if (state.tailOffset() == -1 || state.headOffset() == -1) {
      return 0;
    }

    return (int) Math.max(0L, state.tailOffset() - state.headOffset() + 1);
  }
}
