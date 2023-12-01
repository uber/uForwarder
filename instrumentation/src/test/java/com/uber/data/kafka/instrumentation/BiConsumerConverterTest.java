package com.uber.data.kafka.instrumentation;

import static org.junit.Assert.*;

import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Test;
import org.mockito.Mockito;

public class BiConsumerConverterTest extends FievelTestBase {
  @Test
  public void testUncheck() {
    ThrowingBiConsumer<Boolean, Boolean, IllegalArgumentException> inner =
        Mockito.mock(ThrowingBiConsumer.class);
    BiConsumerConverter.uncheck(inner).accept(true, true);
    Mockito.verify(inner, Mockito.times(1)).accept(true, true);
  }

  @Test(expected = RuntimeException.class)
  public void testUncheckThrowsException() {
    ThrowingBiConsumer<Boolean, Boolean, IllegalArgumentException> inner =
        Mockito.mock(ThrowingBiConsumer.class);
    Mockito.doThrow(new IllegalArgumentException()).when(inner).accept(true, true);
    BiConsumerConverter.uncheck(inner).accept(true, true);
  }
}
