package com.uber.data.kafka.consumerproxy.common;

import com.uber.data.kafka.datatransfer.common.TestUtils;
import com.uber.fievel.testing.base.FievelTestBase;
import java.time.Duration;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class LingerSamplerTest extends FievelTestBase {
  private LingerSampler<String> lingerSampler;
  private Supplier<String> mockSupplier;
  private TestUtils.TestTicker testTicker;

  @Before
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
    Assert.assertEquals("sample", result);
  }

  @Test
  public void testGetSecondTime() {
    String result = lingerSampler.get();
    Mockito.verify(mockSupplier, Mockito.times(1)).get();
    Assert.assertEquals("sample", result);

    lingerSampler.get();
    // return from cache
    Mockito.verify(mockSupplier, Mockito.times(1)).get();
  }

  @Test
  public void testGetAfterLinger() {
    String result = lingerSampler.get();
    Mockito.verify(mockSupplier, Mockito.times(1)).get();
    Assert.assertEquals("sample", result);

    testTicker.add(Duration.ofMinutes(1));
    result = lingerSampler.get();
    // return from cache
    Mockito.verify(mockSupplier, Mockito.times(2)).get();
    Assert.assertEquals("sample", result);
  }
}
