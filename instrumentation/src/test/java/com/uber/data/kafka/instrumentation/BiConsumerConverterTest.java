package com.uber.data.kafka.instrumentation;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class BiConsumerConverterTest {
  @Test
  public void testUncheck() {
    ThrowingBiConsumer<Boolean, Boolean, IllegalArgumentException> inner =
        Mockito.mock(ThrowingBiConsumer.class);
    BiConsumerConverter.uncheck(inner).accept(true, true);
    Mockito.verify(inner, Mockito.times(1)).accept(true, true);
  }

  @Test
  public void testUncheckThrowsException() {
    assertThrows(
        RuntimeException.class,
        () -> {
          ThrowingBiConsumer<Boolean, Boolean, IllegalArgumentException> inner =
              Mockito.mock(ThrowingBiConsumer.class);
          Mockito.doThrow(new IllegalArgumentException()).when(inner).accept(true, true);
          BiConsumerConverter.uncheck(inner).accept(true, true);
        });
  }
}
