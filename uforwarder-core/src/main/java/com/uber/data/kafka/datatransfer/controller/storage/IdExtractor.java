package com.uber.data.kafka.datatransfer.controller.storage;

import java.util.function.Function;

/**
 * IdExtractor extracts id from the given item.
 *
 * @param <K> the type of the id.
 * @param <V> the type of the item to be extracted id from.
 */
public class IdExtractor<K, V> implements IdProvider<K, V> {
  private final Function<V, K> idExtractFunc;

  public IdExtractor(Function<V, K> idExtractFunc) {
    this.idExtractFunc = idExtractFunc;
  }

  @Override
  public K getId(V item) throws Exception {
    return idExtractFunc.apply(item);
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public boolean isRunning() {
    return true;
  }
}
