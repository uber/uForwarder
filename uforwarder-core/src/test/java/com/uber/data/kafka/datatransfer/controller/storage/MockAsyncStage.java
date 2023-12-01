package com.uber.data.kafka.datatransfer.controller.storage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.WatchedEvent;

public class MockAsyncStage<T> extends CompletableFuture<T> implements AsyncStage<T> {
  @Override
  public CompletionStage<WatchedEvent> event() {
    return null;
  }
}
