package com.uber.data.kafka.consumerproxy.worker.processor;

import com.uber.data.kafka.datatransfer.Job;
import com.uber.fievel.testing.base.FievelTestBase;
import com.uber.m3.tally.Counter;
import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class HeadBlockingDetectorTest extends FievelTestBase {
  private AckTrackingQueue ackTrackingQueue;
  private HeadBlockingDetector detector;
  private Job job;
  private Scope scope;
  private TopicPartition topicPartition;

  @Before
  public void setUp() {
    scope = Mockito.mock(Scope.class);
    Counter counter = Mockito.mock(Counter.class);
    Gauge gauge = Mockito.mock(Gauge.class);
    Mockito.when(scope.tagged(ArgumentMatchers.anyMap())).thenReturn(scope);
    Mockito.when(scope.counter(ArgumentMatchers.anyString())).thenReturn(counter);
    Mockito.when(scope.gauge(ArgumentMatchers.anyString())).thenReturn(gauge);
    job = Job.newBuilder().build();
    topicPartition =
        new TopicPartition(
            job.getKafkaConsumerTask().getTopic(), job.getKafkaConsumerTask().getPartition());
    ackTrackingQueue = new LinkedAckTrackingQueue(job, 100, scope);
    detector = HeadBlockingDetector.newBuilder().setCritical(0.9).setMinAckPercent(-0.001).build();
  }

  @Test
  public void testOK() throws InterruptedException {
    for (long offset = 0; offset < 50; ++offset) {
      ackTrackingQueue.receive(offset);
    }
    Optional<BlockingQueue.BlockingMessage> message = detector.detect(scope, ackTrackingQueue);
    Assert.assertFalse(message.isPresent());
  }

  @Test
  public void testBlocked() throws InterruptedException {
    for (long offset = 0; offset < 91; ++offset) {
      ackTrackingQueue.receive(offset);
    }
    Optional<BlockingQueue.BlockingMessage> message = detector.detect(scope, ackTrackingQueue);
    Assert.assertEquals(BlockingQueue.BlockingReason.BLOCKING, message.get().getReason());
  }

  @Test
  public void testBlockedBy8Messages() throws InterruptedException {
    for (long offset = 0; offset < 90; ++offset) {
      ackTrackingQueue.receive(offset);
    }

    Optional<BlockingQueue.BlockingMessage> message = detector.detect(scope, ackTrackingQueue);
    Assert.assertFalse(message.isPresent());

    ackTrackingQueue.receive(90);
    message = detector.detect(scope, ackTrackingQueue);
    Assert.assertEquals(BlockingQueue.BlockingReason.BLOCKING, message.get().getReason());

    // ack most offsets in received offsets
    for (long offset = 5; offset < 10; ++offset) {
      ackTrackingQueue.ack(offset + 1);
    }
    message = detector.detect(scope, ackTrackingQueue);
    Assert.assertEquals(BlockingQueue.BlockingReason.BLOCKING, message.get().getReason());

    // cancel one message to resolve Blocking state
    AckTrackingQueue.State state = ackTrackingQueue.getState();
    Assert.assertTrue(ackTrackingQueue.cancel(state.lowestCancelableOffset() + 1));
    message = detector.detect(scope, ackTrackingQueue);
    Assert.assertFalse(message.isPresent());
  }

  @Test
  public void testDetectEmptyQueue() {
    Optional<BlockingQueue.BlockingMessage> message = detector.detect(scope, ackTrackingQueue);
    Assert.assertFalse(message.isPresent());
  }

  @Test
  public void testNoCancellableMessage() throws InterruptedException {
    ackTrackingQueue.receive(0);
    ackTrackingQueue.cancel(1);
    Optional<BlockingQueue.BlockingMessage> message = detector.detect(scope, ackTrackingQueue);
    Assert.assertFalse(message.isPresent());
  }
}
