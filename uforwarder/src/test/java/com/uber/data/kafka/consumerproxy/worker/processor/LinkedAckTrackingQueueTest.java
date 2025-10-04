package com.uber.data.kafka.consumerproxy.worker.processor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.uber.data.kafka.datatransfer.Job;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class LinkedAckTrackingQueueTest {
  private static final int SIZE = 3;
  private static final AttributeKey ZONE_KEY = new AttributeKey("zone");
  private static final Attribute<String> ZONE_A = new Attribute<>("a");
  private static final Attribute<String> ZONE_B = new Attribute<>("b");
  private LinkedAckTrackingQueue queue;
  private Scope scope;
  private Gauge gauge;
  private Job job;

  @BeforeEach
  public void setUp() {
    scope = Mockito.mock(Scope.class);
    Timer timer = Mockito.mock(Timer.class);
    gauge = Mockito.mock(Gauge.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.timer(ArgumentMatchers.anyString())).thenReturn(timer);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    job = Job.newBuilder().build();
    queue = new LinkedAckTrackingQueue(job, SIZE, scope);
  }

  @Test
  public void testGetTopicPartition() {
    Assertions.assertEquals(
        job.getKafkaConsumerTask().getTopic(), queue.getTopicPartition().topic());
    Assertions.assertEquals(
        job.getKafkaConsumerTask().getPartition(), queue.getTopicPartition().partition());
  }

  @Test
  public void testNegativeCapacity1() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new LinkedAckTrackingQueue(job, 0, scope);
        });
  }

  @Test
  public void testNegativeCapacity2() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new LinkedAckTrackingQueue(job, -5, scope);
        });
  }

  @Test
  public void testInitialValue() {
    assertValues(-1, -1, -1, -1, 0, 0, 0, 0, 0);
    Assertions.assertEquals(SIZE, queue.capacity);
    Assertions.assertEquals(0, queue.offsetStatusMap.size());
    Assertions.assertEquals(0, queue.getState().stats().acked());
  }

  @Test
  public void testReceiveNormally() throws InterruptedException {
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    assertValues(99, 99, 99, 99, 1, 0, 0, 0, 1);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 1, 0, 0);
    queue.receive(100, ImmutableMap.of(ZONE_KEY, ZONE_B));
    assertValues(100, 99, 99, 99, 2, 0, 0, 0, 2);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_B), 1, 0, 0);
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_B));
    assertValues(101, 99, 99, 99, 3, 0, 0, 0, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_B), 2, 0, 0);
  }

  @Test
  public void testReceiveReset() throws InterruptedException {
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    assertValues(99, 99, 99, 99, 1, 0, 0, 0, 1);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 1, 0, 0);
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_A));
    assertValues(101, 99, 99, 99, 2, 0, 0, 0, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 0, 0);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testReceiveBlockedAndAck() throws InterruptedException {
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_B));
    queue.receive(100, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_A));
    new Thread(
            () -> {
              try {
                Thread.sleep(5);
                queue.ack(100);
              } catch (Exception ignored) {
              }
            })
        .start();
    queue.receive(102, ImmutableMap.of(ZONE_KEY, ZONE_A));
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 3, 0, 0);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_B), 0, 0, 0);
    Assertions.assertFalse(queue.notInUse);
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
    Assertions.assertTrue(queue.notInUse);
  }

  @Test
  public void testAck() throws InterruptedException {
    // before receive
    Assertions.assertEquals(AckTrackingQueue.CANNOT_ACK, queue.ack(99));
    assertValues(-1, -1, -1, -1, 0, 0, 0, 0, 0);
    // ack a too large offset
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    Assertions.assertEquals(AckTrackingQueue.CANNOT_ACK, queue.ack(200));
    assertValues(99, 99, 99, 99, 1, 0, 0, 0, 1);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 1, 0, 0);
    // ack a too small offset
    Assertions.assertEquals(AckTrackingQueue.CANNOT_ACK, queue.ack(99));
    assertValues(99, 99, 99, 99, 1, 0, 0, 0, 1);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 1, 0, 0);
    // ack normally
    Assertions.assertEquals(100, queue.ack(100));
    assertValues(99, 100, -1, -1, 0, 0, 0, 0, 0);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 0, 0, 0);
  }

  @Test
  public void testInOrderAck() throws InterruptedException {
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(100);
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_A));
    Assertions.assertEquals(100, queue.ack(100));
    assertValues(101, 100, 100, 100, 2, 0, 0, 0, 2);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 1, 0, 0);
    Assertions.assertEquals(101, queue.ack(101));
    assertValues(101, 101, 101, 101, 1, 0, 0, 0, 1);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 1, 0, 0);
    Assertions.assertEquals(102, queue.ack(102));
    assertValues(101, 102, -1, -1, 0, 0, 0, 0, 0);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 0, 0, 0);
  }

  @Test
  public void testOutOfOrderAck() throws InterruptedException {
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(100);
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_A));
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(101, 99, 99, 99, 3, 1, 0, 1, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 0, 0);
    Assertions.assertEquals(AckTrackingQueue.DUPLICATED_ACK, queue.ack(101));
    assertValues(101, 99, 99, 99, 3, 1, 0, 1, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 0, 0);
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(102));
    assertValues(101, 99, 99, 99, 3, 2, 0, 2, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 1, 0);
    Assertions.assertEquals(102, queue.ack(100));
    assertValues(101, 102, -1, -1, 0, 0, 0, 0, 0);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 0, 0, 0);
  }

  @Test
  public void testOutOfOrderAckWithMetrics() throws InterruptedException {
    Gauge uncommitedGauage = Mockito.mock(Gauge.class);
    Mockito.when(scope.gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED))
        .thenReturn(uncommitedGauage);
    queue.receive(99);
    queue.receive(100);
    queue.receive(103);
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(103, 99, 99, 99, 3, 1, 0, 1, 5);
    Mockito.verify(scope, Mockito.times(1))
        .gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED);
    Mockito.verify(uncommitedGauage, Mockito.times(1)).update(3);

    // commit first message in the queue,
    Assertions.assertEquals(101, queue.ack(100));
    assertValues(103, 101, 103, 103, 1, 0, 0, 0, 1);
    Mockito.verify(scope, Mockito.times(2))
        .gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED);
    Mockito.verify(uncommitedGauage, Mockito.times(1)).update(1);

    queue.publishMetrics();
    Mockito.verify(scope, Mockito.times(3))
        .gauge(AckTrackingQueue.MetricNames.IN_MEMORY_UNCOMMITTED);
    Mockito.verify(uncommitedGauage, Mockito.times(2)).update(1);
  }

  @Test
  public void testOutOfOrderAckWithGap() throws InterruptedException {
    queue.receive(99);
    queue.receive(110);
    queue.receive(121);
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(111));
    assertValues(121, 99, 99, 99, 3, 1, 0, 11, 23);
    Assertions.assertEquals(AckTrackingQueue.DUPLICATED_ACK, queue.ack(111));
    assertValues(121, 99, 99, 99, 3, 1, 0, 11, 23);
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(122));
    assertValues(121, 99, 99, 99, 3, 2, 0, 22, 23);
    Assertions.assertEquals(122, queue.ack(100));
    assertValues(121, 122, -1, -1, 0, 0, 0, 0, 0);
  }

  // this should never happen, but in case something is wrong, we test this case
  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testAckBlockedAndAck() throws InterruptedException {
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    new Thread(
            () -> {
              try {
                Thread.sleep(5);
                queue.ack(100);
              } catch (Exception ignored) {
              }
            })
        .start();
    queue.receive(102);
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(103));
    assertValues(102, 100, 100, 100, 3, 1, 0, 2, 3);
    Assertions.assertFalse(queue.notInUse);
  }

  // this should never happen, but in case something is wrong, we test this case
  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testAckBlockedAndNotInUse() throws InterruptedException {
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
    Assertions.assertEquals(AckTrackingQueue.CANNOT_ACK, queue.ack(103));
    assertValues(101, 99, 99, 99, 3, 0, 0, 0, 3);
    Assertions.assertTrue(queue.notInUse);
  }

  @Test
  public void testNack() throws InterruptedException {
    // before receive
    Assertions.assertFalse(queue.nack(99));
    assertValues(-1, -1, -1, -1, 0, 0, 0, 0, 0);
    // nack a too large offset
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    Assertions.assertFalse(queue.nack(200));
    assertValues(99, 99, 99, 99, 1, 0, 0, 0, 1);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 1, 0, 0);
    // nack a too small offset
    Assertions.assertFalse(queue.nack(99));
    assertValues(99, 99, 99, 99, 1, 0, 0, 0, 1);
    // nack normally
    Assertions.assertTrue(queue.nack(100));
    assertValues(99, 99, 99, 99, 1, 0, 0, 0, 1);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 1, 0, 0);
    // nack a nacked offset
    Assertions.assertFalse(queue.nack(100));
    assertValues(99, 99, 99, 99, 1, 0, 0, 0, 1);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testNackBlockedAndAck() throws InterruptedException {
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    new Thread(
            () -> {
              try {
                Thread.sleep(5);
                queue.ack(100);
              } catch (Exception ignored) {
              }
            })
        .start();
    Assertions.assertTrue(queue.nack(102));
    assertValues(101, 99, 99, 99, 3, 0, 0, 0, 3);
    Assertions.assertFalse(queue.notInUse);
  }

  @SuppressWarnings("ForbidTimedWaitInTests") // Initial enrollment
  @Test
  public void testNackNonBlockingAndNotInUse() throws InterruptedException {
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assertions.assertFalse(queue.nack(103));
    assertValues(101, 99, 99, 99, 3, 0, 0, 0, 3);
    Assertions.assertFalse(queue.notInUse);
    queue.markAsNotInUse();
    Assertions.assertTrue(queue.notInUse);
  }

  @Test
  public void testNackAfterAck() throws InterruptedException {
    // set up
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(101, 99, 99, 99, 3, 1, 0, 1, 3);
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(102));
    assertValues(101, 99, 99, 99, 3, 2, 0, 2, 3);
    // test
    Assertions.assertFalse(queue.nack(101));
    Assertions.assertFalse(queue.nack(102));
  }

  @Test
  public void testAckAfterNack() throws InterruptedException {
    // set up
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(100);
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_A));
    Assertions.assertTrue(queue.nack(100));
    Assertions.assertTrue(queue.nack(101));
    Assertions.assertTrue(queue.nack(102));
    assertValues(101, 99, 99, 99, 3, 0, 0, 0, 3);
    // test: reuse the code
    testOutOfOrderAck();
  }

  @Test
  public void testAckAfterCancel() throws InterruptedException {
    // set up
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(100, ImmutableMap.of(ZONE_KEY, ZONE_B));
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_A));
    Assertions.assertTrue(queue.cancel(100));
    assertValues(101, 99, 99, 100, 3, 0, 1, 0, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 0, 1);
    Assertions.assertEquals(100, queue.ack(100));
    assertValues(101, 100, 100, 100, 2, 0, 0, 0, 2);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 1, 0, 0);
  }

  @Test
  public void testAckNextAfterCancel() throws InterruptedException {
    // set up
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(100, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_B));
    Assertions.assertTrue(queue.cancel(100));
    assertValues(101, 99, 99, 100, 3, 0, 1, 0, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 0, 1);
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(101, 99, 99, 101, 3, 1, 1, 1, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 1, 1);
  }

  @Test
  public void testCancelAfterAck() throws InterruptedException {
    // set up
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assertions.assertEquals(100, queue.ack(100));
    assertValues(101, 100, 100, 100, 2, 0, 0, 0, 2);
    Assertions.assertFalse(queue.cancel(100));
    assertValues(101, 100, 100, 100, 2, 0, 0, 0, 2);
  }

  @Test
  public void testCancelMoveOffset() throws InterruptedException {
    // set up
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(100, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_B));
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(101));
    assertValues(101, 99, 99, 99, 3, 1, 0, 1, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 1, 0);
    Assertions.assertTrue(queue.cancel(100));
    assertValues(101, 99, 99, 101, 3, 1, 1, 1, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 1, 1);
  }

  @Test
  public void testCancelAfterNack() throws InterruptedException {
    // set up
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assertions.assertEquals(true, queue.nack(100));
    assertValues(101, 99, 99, 99, 3, 0, 0, 0, 3);
    Assertions.assertTrue(queue.cancel(100));
    assertValues(101, 99, 99, 100, 3, 0, 1, 0, 3);
  }

  @Test
  public void testCancelAll() throws InterruptedException {
    // set up
    queue.receive(99, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(100, ImmutableMap.of(ZONE_KEY, ZONE_A));
    queue.receive(101, ImmutableMap.of(ZONE_KEY, ZONE_B));
    queue.cancel(100);
    queue.cancel(101);
    queue.cancel(102);
    assertValues(101, 99, 99, -1, 3, 0, 3, 0, 3);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_A), 2, 0, 2);
    assertAttributeStatus(ImmutableMap.of(ZONE_KEY, ZONE_B), 1, 0, 1);
  }

  @Test
  public void testNackAfterCancel() throws InterruptedException {
    // set up
    queue.receive(99);
    queue.receive(100);
    queue.receive(101);
    Assertions.assertTrue(queue.cancel(100));
    assertValues(101, 99, 99, 100, 3, 0, 1, 0, 3);
    Assertions.assertEquals(false, queue.nack(100));
    assertValues(101, 99, 99, 100, 3, 0, 1, 0, 3);
  }

  @Test
  public void testPublishMetrics() {
    queue.publishMetrics();
    Mockito.verify(scope, Mockito.times(1)).gauge("tracking-queue.blocking.load-factor");
    Mockito.verify(scope, Mockito.times(1)).gauge("tracking-queue.blocking.load-factor-exclusive");
    Mockito.verify(scope, Mockito.times(1)).gauge("tracking-queue.blocking.acked");
    Mockito.verify(scope, Mockito.times(1)).gauge("tracking-queue.blocking.cancelled");
  }

  @Test
  public void testReceiveGap() throws InterruptedException {
    // set up
    queue.receive(99);
    queue.receive(102);
    Assertions.assertTrue(queue.nack(100));
    Assertions.assertFalse(queue.nack(101));
    Assertions.assertFalse(queue.nack(102));
    Assertions.assertEquals(AckTrackingQueue.IN_MEMORY_ACK_ONLY, queue.ack(103));
    assertValues(102, 99, 99, 99, 2, 1, 0, 3, 4);
    Assertions.assertEquals(103, queue.ack(100));
    assertValues(102, 103, -1, -1, 0, 0, 0, 0, 0);
  }

  @Test
  public void testReceiveWithGap() throws InterruptedException {
    // set up
    int capacity = 1000;
    LinkedAckTrackingQueue linkedQueue = new LinkedAckTrackingQueue(job, capacity, scope);
    List<Action> actions = generateActions(capacity, 10000, true);
    for (Action action : actions) {
      switch (action.actionType) {
        case Ack:
          linkedQueue.ack(action.offset);
          break;
        case Nack:
          linkedQueue.nack(action.offset);
          break;
        case Receive:
          linkedQueue.receive(action.offset);
          break;
      }
    }
  }

  List<Action> generateActions(int capacity, int numAction, boolean withGap) {
    long highestReceived = 1001;
    Random r = new Random(12345);
    ArrayList<Long> received = new ArrayList<>();
    ArrayList<Long> acked = new ArrayList<>();
    List<Action> result = new ArrayList<>();
    int numActionType = ActionType.values().length;
    int i = 0;
    int index;
    while (i < numAction) {
      ActionType actionType = ActionType.values()[r.nextInt(numActionType)];
      Action action = new Action();
      action.actionType = actionType;
      switch (action.actionType) {
        case Ack:
          if (received.isEmpty()) {
            continue;
          }

          int x = r.nextInt(10);
          if (x < 2) {
            // duplicate ack
            action.offset = acked.get(r.nextInt(acked.size()));
          } else {
            // first ack
            index = r.nextInt(received.size());
            action.offset = received.get(index) + 1;
            acked.add(received.remove(index));
          }
          break;
        case Nack:
          if (received.isEmpty()) {
            continue;
          }
          index = r.nextInt(received.size());
          action.offset = received.get(index) + 1;
          break;
        case Receive:
          if (received.size() == 0 || highestReceived - received.get(0) + 1 < capacity) {
            highestReceived++;
            if (withGap) {
              highestReceived += r.nextInt(10);
            }
            received.add(highestReceived);
            action.offset = highestReceived;
          } else {
            continue;
          }
          break;
      }
      result.add(action);
      i++;
    }
    return result;
  }

  class Action {
    ActionType actionType;
    long offset;
  }

  enum ActionType {
    Receive,
    Ack,
    Nack
  }

  private void assertValues(
      long highestReceivedOffset,
      long highestCommittedOffset,
      long headOffset,
      long lowestCancelableOffset,
      int size,
      int ackedCount,
      int canceled,
      int width,
      int length) {
    Assertions.assertEquals(highestReceivedOffset, queue.highestReceivedOffset);
    Assertions.assertEquals(highestCommittedOffset, queue.highestCommittedOffset);
    Assertions.assertEquals(headOffset, queue.getState().headOffset());
    Assertions.assertEquals(lowestCancelableOffset, queue.getState().lowestCancelableOffset());
    Assertions.assertEquals(size, queue.getState().stats().size());
    Assertions.assertEquals(ackedCount, queue.getState().stats().acked());
    Assertions.assertEquals(canceled, queue.getState().stats().canceled());
    Assertions.assertEquals(width, width(queue.getState()));
    Assertions.assertEquals(length, length(queue.getState()));
  }

  private void assertAttributeStatus(
      Map<AttributeKey, Attribute> attributes, int size, int acked, int canceled) {
    Attribute zone = attributes.get(ZONE_KEY);
    AckTrackingQueue.Stats stats = queue.getState().attributesStats().get(ZONE_KEY).get(zone);
    Assertions.assertEquals(size, stats.size());
    Assertions.assertEquals(acked, stats.acked());
    Assertions.assertEquals(canceled, stats.canceled());
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
