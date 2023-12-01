package com.uber.data.kafka.instrumentation;

import com.uber.fievel.testing.base.FievelTestBase;
import java.util.concurrent.ExecutionException;
import org.junit.Assert;
import org.junit.Test;

public class DirectSupplierTest extends FievelTestBase {
  @Test
  public void testSupply() throws Exception {
    Assert.assertTrue(DirectSupplier.supply(() -> true).toCompletableFuture().get());
  }

  @Test(expected = ExecutionException.class)
  public void testSupplyWithException() throws Exception {
    DirectSupplier.supply(
            () -> {
              throw new RuntimeException();
            })
        .toCompletableFuture()
        .get();
  }
}
