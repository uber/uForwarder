package com.uber.data.kafka.consumerproxy.common;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import java.time.Duration;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LingerSamplerTest {
  private LingerSampler<String> lingerSampler;
  private Supplier<String> mockSupplier;
  private TestUtils.TestTicker testTicker;

  @BeforeEach
  public void setUp() {
    testTicker = new TestUtils.TestTicker();
    mockSupplier = Mockito.mock(Supplier.class);
    Mockito.when(mockSupplier.get()).thenReturn("sample");
    lingerSampler = new LingerSampler<>(testTicker, 1000, mockSupplier);
  }

  @Test
  public void testGetFirstTime() {
    String result = lingerSampler.get();
    Mockito.verify(mockSupplier, Mockito.times(1)).get();
    Assertions.assertEquals("sample", result);
  }

  @Test
  public void testGetSecondTime() {
    String result = lingerSampler.get();
    Mockito.verify(mockSupplier, Mockito.times(1)).get();
    Assertions.assertEquals("sample", result);

    lingerSampler.get();
    // return from cache
    Mockito.verify(mockSupplier, Mockito.times(1)).get();
  }

  @Test
  public void testGetAfterLinger() {
    String result = lingerSampler.get();
    Mockito.verify(mockSupplier, Mockito.times(1)).get();
    Assertions.assertEquals("sample", result);

    testTicker.add(Duration.ofMinutes(1));
    result = lingerSampler.get();
    // return from cache
    Mockito.verify(mockSupplier, Mockito.times(2)).get();
    Assertions.assertEquals("sample", result);
  }
}
