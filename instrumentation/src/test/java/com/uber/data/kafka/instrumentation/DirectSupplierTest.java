package com.uber.data.kafka.instrumentation;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DirectSupplierTest {
  @Test
  public void testSupply() throws Exception {
    Assertions.assertTrue(DirectSupplier.supply(() -> true).toCompletableFuture().get());
  }

  @Test
  public void testSupplyWithException() {
    assertThrows(
        ExecutionException.class,
        () ->
            DirectSupplier.supply(
                    () -> {
                      throw new RuntimeException();
                    })
                .toCompletableFuture()
                .get());
  }
}
