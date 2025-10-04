package com.uber.data.kafka.consumerproxy.worker.processor;

import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.uber.data.kafka.consumerproxy.worker.limiter.InflightLimiter;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class NestedPermitTest {
  @Test
  public void testComplete() {
    // Given
    InflightLimiter.Result result = mock(InflightLimiter.Result.class);
    InflightLimiter.Permit permit1 = mock(InflightLimiter.Permit.class);
    InflightLimiter.Permit permit2 = mock(InflightLimiter.Permit.class);
    InflightLimiter.Permit permit3 = mock(InflightLimiter.Permit.class);
    Consumer consumer1 = mock(Consumer.class);
    Consumer consumer2 = mock(Consumer.class);
    Consumer consumer3 = mock(Consumer.class);
    List<InflightLimiter.Permit> permits = ImmutableList.of(permit1, permit2, permit3);
    List<Consumer> consumers = ImmutableList.of(consumer1, consumer2, consumer3);
    NestedPermit nestedPermit = new NestedPermit(permits, consumers);

    // When
    boolean result1 = nestedPermit.complete(result);
    boolean result2 = nestedPermit.complete(result);

    // Then
    Assertions.assertTrue(result1);
    Assertions.assertFalse(result2);
    Mockito.verify(permit1).complete(result);
    Mockito.verify(permit2).complete(result);
    Mockito.verify(permit3).complete(result);
    Mockito.verify(consumer1).accept(result);
    Mockito.verify(consumer2).accept(result);
    Mockito.verify(consumer3).accept(result);
  }
}
